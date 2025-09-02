use std::collections::HashMap;
use std::ops::Index;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::error::{DbError, DbResult};
use crate::storage::BlockId;

/// LockTable - currently follows an original design, uses a single lock for all blocks
pub struct LockTable {
    locks: Mutex<HashMap<BlockId, LockState>>, // TODO single mutex
    cond: Condvar, // TODO single condvar
    max_time: u64,
}

struct LockState {
    s_lock_tx_ids: Vec<i32>,
    x_lock_tx_id: Option<i32>,
    pub x_lock_request_count: i32, // TODO currently we do not respect X lock order request. Will fix when deadlock detection is imlemented
}

impl LockState {
    fn new() -> Self {
        LockState {
            s_lock_tx_ids: vec![],
            x_lock_tx_id: None,
            x_lock_request_count: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.s_lock_tx_ids.is_empty() && self.x_lock_request_count == 0 && self.x_lock_tx_id.is_none()
    }

    fn add_s_lock(&mut self, tx_id: i32) {
        self.s_lock_tx_ids.push(tx_id);
    }

    fn set_x_lock(&mut self, tx_id: i32) {
        self.x_lock_tx_id = Some(tx_id);
    }

    fn remove_tx_id(&mut self, tx_id: i32) {
        self.s_lock_tx_ids.retain(|&x| x != tx_id);
        if let Some(current_x_lock) = self.x_lock_tx_id {
            if current_x_lock == tx_id {
                self.x_lock_tx_id = None;
            }
        }
    }

    fn has_any_locks(&self) -> bool {
        self.has_s_locks() || self.has_x_lock()
    }

    fn has_any_other_locks_except(&self, tx_id: i32) -> bool {
        self.has_s_locks_except(tx_id) || self.has_x_lock()
    }

    fn has_s_locks_except(&self, tx_id: i32) -> bool {
        self.s_lock_tx_ids.iter().find(|x| **x != tx_id).is_some()
    }

    fn has_s_locks(&self) -> bool {
        !self.s_lock_tx_ids.is_empty()
    }

    fn has_x_lock_or_x_request(&self) -> bool {
        self.x_lock_tx_id.is_some() || self.x_lock_request_count > 0
    }

    fn has_x_lock(&self) -> bool {
        self.x_lock_tx_id.is_some()
    }
}

impl LockTable {
    pub fn new() -> Self {
        Self::with_timeout(1000)
    }

    pub fn with_timeout(max_time: u64) -> Self {
        Self {
            locks: Mutex::new(HashMap::new()),
            cond: Condvar::new(),
            max_time,
        }
    }

    /// Acquire a shared lock on the specified block.
    /// If an exclusive lock exists on the block, then the method waits
    /// until the lock is released. If the wait times out, then an
    /// exception is thrown.
    pub fn lock_s(&self, blk: &BlockId, tx_id: i32) -> DbResult<()> {
        let start_time = Instant::now();
        let max_duration = Duration::from_millis(self.max_time);

        let mut locks = self.locks.lock().unwrap();
        locks.entry(blk.clone()).or_insert_with(|| LockState::new());

        while locks.get(blk).unwrap().has_x_lock_or_x_request() && !self.waiting_too_long(start_time) {
            let result = self.cond.wait_timeout(locks, max_duration).unwrap();
            locks = result.0;

            let lock_state = locks.entry(blk.clone()).or_insert_with(|| LockState::new()); // TODO clone and new every op
            if result.1.timed_out() && lock_state.has_x_lock_or_x_request() {
                return Err(DbError::LockAbort);
            }
        }

        let lock_state = locks.entry(blk.clone()).or_insert_with(|| LockState::new()); // TODO clone and new every op
        if lock_state.has_x_lock_or_x_request() {
            return Err(DbError::LockAbort);
        }

        lock_state.add_s_lock(tx_id);
        Ok(())
    }

    /// Upgrades lock from S to X
    /// This method must be called if S lock has been already taken for the provided block
    pub fn upgrade_to_x(&self, blk: &BlockId, tx_id: i32) -> DbResult<()> {
        let start_time = Instant::now();
        let max_duration = Duration::from_millis(self.max_time);

        let mut locks = self.locks.lock().unwrap();
        let lock_state = locks.entry(blk.clone()).or_insert_with(|| LockState::new());
        lock_state.x_lock_request_count += 1;

        while locks.get(blk).unwrap().has_any_other_locks_except(tx_id) && !self.waiting_too_long(start_time) {
            let result = self.cond.wait_timeout(locks, max_duration).unwrap();
            locks = result.0;

            if result.1.timed_out() && locks.get(blk).unwrap().has_any_other_locks_except(tx_id) {
                locks.get_mut(blk).unwrap().x_lock_request_count -= 1;
                return Err(DbError::LockAbort);
            }
        }

        if locks.get(blk).unwrap().has_any_other_locks_except(tx_id) {
            locks.get_mut(blk).unwrap().x_lock_request_count -= 1;
            return Err(DbError::LockAbort);
        }

        locks.get_mut(blk).unwrap().set_x_lock(tx_id);
        locks.get_mut(blk).unwrap().x_lock_request_count -= 1;
        Ok(())
    }

    /// Acquire an exclusive lock on the specified block.
    pub fn lock_x(&self, blk: &BlockId, tx_id: i32) -> DbResult<()> {
        let start_time = Instant::now();
        let max_duration = Duration::from_millis(self.max_time);

        let mut locks = self.locks.lock().unwrap();
        let lock_state = locks.entry(blk.clone()).or_insert_with(|| LockState::new());
        lock_state.x_lock_request_count += 1;

        while locks.get(blk).unwrap().has_any_locks() && !self.waiting_too_long(start_time) {
            let result = self.cond.wait_timeout(locks, max_duration).unwrap();
            locks = result.0;

            if result.1.timed_out() && locks.get(blk).unwrap().has_any_locks() {
                locks.get_mut(blk).unwrap().x_lock_request_count -= 1;
                return Err(DbError::LockAbort);
            }
        }

        if locks.get(blk).unwrap().has_any_locks() {
            locks.get_mut(blk).unwrap().x_lock_request_count -= 1;
            return Err(DbError::LockAbort);
        }

        locks.get_mut(blk).unwrap().set_x_lock(tx_id);
        locks.get_mut(blk).unwrap().x_lock_request_count -= 1;
        Ok(())
    }

    /// Release the lock on the specified block.
    /// If this transaction is the only one with a lock on that block,
    /// then the lock is removed. Otherwise, the lock value is decremented.
    pub fn unlock(&self, blk: &BlockId, tx_id: i32) {
        let mut locks = self.locks.lock().unwrap();
        let mut lock_state = locks.get_mut(blk).expect(&format!("Unlocking but there is not entry for block {}", blk));

        lock_state.remove_tx_id(tx_id);
        if lock_state.is_empty() {
            locks.remove(blk);
        }
        // TODO notify if?
        self.cond.notify_all();
    }

    fn waiting_too_long(&self, start_time: Instant) -> bool {
        start_time.elapsed().as_millis() > self.max_time as u128
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use rand::Rng;

    #[test]
    fn test_lock_table() {
        let lock_table = Arc::new(LockTable::new());
        let blk = BlockId::new("testfile".to_string(), 1);

        lock_table.lock_s(&blk, 0).unwrap();

        let lt_clone = Arc::clone(&lock_table);
        let blk_clone = blk.clone();
        let handle = thread::spawn(move || {
            let result = lt_clone.lock_x(&blk_clone, 1);
            assert!(result.is_ok());
        });

        thread::sleep(Duration::from_millis(100));

        lock_table.unlock(&blk, 0);

        handle.join().unwrap();

        lock_table.unlock(&blk, 1);

        let lt_clone = Arc::clone(&lock_table);
        let blk_clone = blk.clone();
        let handle = thread::spawn(move || {
            let result = lt_clone.lock_s(&blk_clone, 2);
            assert!(result.is_ok());
            lt_clone.unlock(&blk_clone, 2);
        });

        thread::sleep(Duration::from_millis(100));

        handle.join().unwrap();
    }

    #[test]
    fn test_x_lock_exclusivity() {
        let lock_table = Arc::new(LockTable::new());
        let blk = BlockId::new("testfile".to_string(), 1);

        lock_table.lock_x(&blk, 0).unwrap();

        let lock_table_clone = Arc::clone(&lock_table);
        let blk_clone = blk.clone();
        let handle = thread::spawn(move || {
            let result = lock_table_clone.lock_x(&blk_clone, 1);
            assert!(!result.is_ok());
        });

        handle.join().unwrap();

        let lt_clone = Arc::clone(&lock_table);
        let blk_clone = blk.clone();
        let handle = thread::spawn(move || {
            let result = lt_clone.lock_s(&blk_clone, 2);
            assert!(result.is_ok());
            lt_clone.unlock(&blk_clone, 2);
        });

        thread::sleep(Duration::from_millis(100));

        lock_table.unlock(&blk, 0);

        handle.join().unwrap();
    }

    #[test]
    fn test_upgrade_from_s_to_x() {
        let lock_table = LockTable::with_timeout(100);
        let blk = BlockId::new("testfile".to_string(), 1);

        lock_table.lock_s(&blk, 0).unwrap();
        lock_table.upgrade_to_x(&blk, 0).unwrap();
        lock_table.unlock(&blk, 0);

        lock_table.lock_s(&blk, 1).unwrap();
        lock_table.upgrade_to_x(&blk, 1).unwrap();
        lock_table.unlock(&blk, 1);

        lock_table.lock_s(&blk, 2).unwrap();
        lock_table.lock_s(&blk, 3).unwrap();
        lock_table.unlock(&blk, 2);
        lock_table.unlock(&blk, 3);
    }

    #[test]
    fn test_upgrade_from_2() {
        let lock_table = LockTable::with_timeout(100);
        let blk = BlockId::new("testfile".to_string(), 1);

        lock_table.lock_s(&blk, 0).unwrap();

        lock_table.lock_s(&blk, 1).unwrap();
        
        assert!(matches!(lock_table.lock_x(&blk, 2), Err(DbError::LockAbort)));

        // two S locks, can't upgrade
        assert!(matches!(lock_table.upgrade_to_x(&blk, 0), Err(DbError::LockAbort)));

        lock_table.unlock(&blk, 1);

        lock_table.upgrade_to_x(&blk, 0).unwrap();

        assert!(matches!(lock_table.lock_x(&blk, 3), Err(DbError::LockAbort)));
    }

    #[test]
    fn test_s_abort_if_x_locked() {
        let lock_table = LockTable::with_timeout(100);
        let blk = BlockId::new("testfile".to_string(), 1);

        lock_table.lock_x(&blk, 0).unwrap();

        let result = lock_table.lock_s(&blk, 1);
        assert!(matches!(result, Err(DbError::LockAbort)));

        lock_table.unlock(&blk, 0);
    }

    #[test]
    fn test_lock_table_s_stress_single_block() {
        const NUM_THREADS: usize = 5;
        const OPERATIONS_PER_THREAD: usize = 10_000;
        
        let lock_table = Arc::new(LockTable::new());
        let tx_id_clone = Arc::new(AtomicI32::new(0));
        
        let mut handles = Vec::new();
        for _ in 0..NUM_THREADS {
            let lock_table_clone = Arc::clone(&lock_table);
            let time_clone = Arc::clone(&tx_id_clone);
            
            let handle = thread::spawn(move || {

                for _ in 0..OPERATIONS_PER_THREAD {
                    let tx_id = time_clone.fetch_add(1, Ordering::Relaxed);
                    let blk = BlockId::new("test_file".to_string(), 0);
                    
                    lock_table_clone.lock_s(&blk, tx_id).unwrap();
                    thread::sleep(Duration::from_micros(2));
                    lock_table_clone.unlock(&blk, tx_id);
                }
            });
            
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_lock_table_x_stress_single_block() {
        const NUM_THREADS: usize = 5;
        const OPERATIONS_PER_THREAD: usize = 10_000;
        
        let counter = Arc::new(AtomicI32::new(0));
        let time = Arc::new(AtomicI32::new(0));
        let lock_table = Arc::new(LockTable::new());
        
        let mut handles = Vec::new();
        for _ in 0..NUM_THREADS {
            let lock_table_clone = Arc::clone(&lock_table);
            let tx_id_clone = Arc::clone(&time);
            let counter_clone = Arc::clone(&counter);

            let handle = thread::spawn(move || {
                for _ in 0..OPERATIONS_PER_THREAD {
                    let blk = BlockId::new("test_file".to_string(), 1);
                    let tx_id = tx_id_clone.fetch_add(1, Ordering::Relaxed);
                    lock_table_clone.lock_x(&blk, tx_id).unwrap();
                    let current = counter_clone.load(Ordering::Relaxed);
                    thread::sleep(Duration::from_micros(2));
                    counter_clone.store(current + 1, Ordering::Relaxed);
                    lock_table_clone.unlock(&blk, tx_id);
                }
            });
            
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(counter.load(Ordering::Relaxed), (OPERATIONS_PER_THREAD * NUM_THREADS) as i32);
    }

    #[test]
    fn test_lock_table_mixed_pattern_single_block() {
        const NUM_THREADS: usize = 5;
        const OPERATIONS_PER_THREAD: usize = 10_000;

        let time = Arc::new(AtomicI32::new(0));
        let lock_table = Arc::new(LockTable::new());
        let counter = Arc::new(AtomicI32::new(0));
        
        let mut handles = Vec::new();
        for _ in 0..NUM_THREADS {
            let lock_table_clone = Arc::clone(&lock_table);
            let time_clone = Arc::clone(&time);
            let counter_clone = Arc::clone(&counter);
            
            let handle = thread::spawn(move || {
                let mut rng = rand::rng();
                let mut incrments = 0;
                
                for _ in 0..OPERATIONS_PER_THREAD {
                    let tx_id = time_clone.fetch_add(1, Ordering::Relaxed);
                    let blk = BlockId::new("test_file".to_string(), 0);
                    
                    if rng.random_bool(0.5) {
                        lock_table_clone.lock_x(&blk, tx_id).unwrap();

                        let current = counter_clone.load(Ordering::Relaxed);
                        thread::sleep(Duration::from_micros(2));
                        counter_clone.store(current + 1, Ordering::Relaxed);
                        incrments += 1;
                    } else {
                        lock_table_clone.lock_s(&blk, tx_id).unwrap();
                    }
                    thread::sleep(Duration::from_micros(1));
                    lock_table_clone.unlock(&blk, tx_id);
                }
                incrments
            });
            
            handles.push(handle);
        }

        let mut total_increments = 0;
        for handle in handles {
            total_increments += handle.join().unwrap();
        }
        assert_eq!(total_increments, counter.load(Ordering::Relaxed));
    }

/*    
    // TODO currently, it doesn't work because we deadlock in the case when two threads do upgrade S -> X for same block
    #[test]
    fn test_lock_table_x_stress_single_block_with_upgrade() {
        const NUM_THREADS: usize = 2;
        const OPERATIONS_PER_THREAD: usize = 10_000;
        
        let counter = Arc::new(AtomicI32::new(0));
        let time = Arc::new(AtomicI32::new(0));
        let lock_table = Arc::new(LockTable::new());
        
        let mut handles = Vec::new();
        for _ in 0..NUM_THREADS {
            let lock_table_clone = Arc::clone(&lock_table);
            let tx_id_clone = Arc::clone(&time);
            let counter_clone = Arc::clone(&counter);

            let handle = thread::spawn(move || {
                let mut increments = 0;
                let mut rng = rand::rng();

                for _ in 0..OPERATIONS_PER_THREAD {    
                    let blk = BlockId::new("test_file".to_string(), 1);
                    let tx_id = tx_id_clone.fetch_add(1, Ordering::Relaxed);
                    if rng.random_bool(0.5) {
                        lock_table_clone.lock_x(&blk, tx_id).unwrap();
                        let current = counter_clone.load(Ordering::Relaxed);
                        thread::sleep(Duration::from_micros(2));
                        counter_clone.store(current + 1, Ordering::Relaxed);
                        increments += 1;
                    } else {
                        lock_table_clone.lock_s(&blk, tx_id).unwrap();

                        if rng.random_bool(0.5) {
                            lock_table_clone.upgrade_to_x(&blk, tx_id).unwrap();
                            
                            let current = counter_clone.load(Ordering::Relaxed);
                            thread::sleep(Duration::from_micros(2));
                            counter_clone.store(current + 1, Ordering::Relaxed);

                            increments += 1;
                        }
                    }
                    lock_table_clone.unlock(&blk, tx_id);
                }
                increments
            });
            
            handles.push(handle);
        }
        
        let mut total_increments = 0;
        for handle in handles {
            total_increments += handle.join().unwrap();
        }

        assert_eq!(counter.load(Ordering::Relaxed), total_increments as i32);
    } */
}
