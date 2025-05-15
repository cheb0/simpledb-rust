use std::collections::HashMap;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::error::{DbError, DbResult};
use crate::storage::BlockId;

/// LockTable - currently follows an original design, uses a single lock for all blocks. Need to redo
pub struct LockTable {
    locks: Mutex<HashMap<BlockId, i32>>,
    cond: Condvar,
    max_time: u64,
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
    pub fn lock_shared(&self, blk: &BlockId) -> DbResult<()> {
        let start_time = Instant::now();
        let max_duration = Duration::from_millis(self.max_time);
        
        let mut locks = self.locks.lock().unwrap();
        
        while self.has_exclusive_lock(&locks, blk) && !self.waiting_too_long(start_time) {
            let result = self.cond.wait_timeout(locks, max_duration).unwrap();
            locks = result.0;
            
            if result.1.timed_out() && self.has_exclusive_lock(&locks, blk) {
                return Err(DbError::LockAbort);
            }
        }
        
        if self.has_exclusive_lock(&locks, blk) {
            return Err(DbError::LockAbort);
        }
        
        let val = self.get_lock_val(&locks, blk);
        locks.insert(blk.clone(), val + 1);
        
        Ok(())
    }

    /// Acquire an exclusive lock on the specified block.
    /// If a shared or exclusive lock exists on the block, then the method waits
    /// until the lock is released.
    pub fn lock_exclusive(&self, blk: &BlockId) -> DbResult<()> {
        let start_time = Instant::now();
        let max_duration = Duration::from_millis(self.max_time);
        
        let mut locks = self.locks.lock().unwrap();
        
        while self.has_other_shared_locks(&locks, blk) && !self.waiting_too_long(start_time) {
            let result = self.cond.wait_timeout(locks, max_duration).unwrap();
            locks = result.0;
            
            if result.1.timed_out() && self.has_other_shared_locks(&locks, blk) {
                return Err(DbError::LockAbort);
            }
        }
        
        if self.has_other_shared_locks(&locks, blk) {
            return Err(DbError::LockAbort);
        }
        
        locks.insert(blk.clone(), -1);
        
        Ok(())
    }

    /// Release the lock on the specified block.
    /// If this transaction is the only one with a lock on that block,
    /// then the lock is removed. Otherwise, the lock value is decremented.
    pub fn unlock(&self, blk: &BlockId) {
        let mut locks = self.locks.lock().unwrap();
        
        let val = self.get_lock_val(&locks, blk);
        if val > 1 {
            locks.insert(blk.clone(), val - 1);
        } else {
            locks.remove(blk);
            self.cond.notify_all();
        }
    }

    fn has_exclusive_lock(&self, locks: &HashMap<BlockId, i32>, blk: &BlockId) -> bool {
        self.get_lock_val(locks, blk) < 0
    }

    fn has_other_shared_locks(&self, locks: &HashMap<BlockId, i32>, blk: &BlockId) -> bool {
        self.get_lock_val(locks, blk) > 1
    }

    fn waiting_too_long(&self, start_time: Instant) -> bool {
        start_time.elapsed().as_millis() > self.max_time as u128
    }

    fn get_lock_val(&self, locks: &HashMap<BlockId, i32>, blk: &BlockId) -> i32 {
        *locks.get(blk).unwrap_or(&0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_lock_table() {
        let lock_table = Arc::new(LockTable::new());
        let blk = BlockId::new("testfile".to_string(), 1);
        
        lock_table.lock_shared(&blk).unwrap();
        
        let lt_clone = Arc::clone(&lock_table);
        let blk_clone = blk.clone();
        let handle = thread::spawn(move || {
            let result = lt_clone.lock_exclusive(&blk_clone);
            assert!(result.is_ok());
        });
        
        thread::sleep(Duration::from_millis(100));
        
        lock_table.unlock(&blk);
        
        handle.join().unwrap();
        
        let lt_clone = Arc::clone(&lock_table);
        let blk_clone = blk.clone();
        let handle = thread::spawn(move || {
            let result = lt_clone.lock_shared(&blk_clone);
            assert!(result.is_ok());
            lt_clone.unlock(&blk_clone);
        });
        
        thread::sleep(Duration::from_millis(100));
        
        lock_table.unlock(&blk);
        
        handle.join().unwrap();
    }

    #[test]
    fn test_lock_timeout() {
        let lock_table = LockTable::with_timeout(100);
        let blk = BlockId::new("testfile".to_string(), 1);
        
        lock_table.lock_exclusive(&blk).unwrap();
        
        let result = lock_table.lock_shared(&blk);
        assert!(matches!(result, Err(DbError::LockAbort)));
        
        lock_table.unlock(&blk);
    }
} 