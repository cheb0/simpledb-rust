use std::collections::HashMap;
use std::sync::Arc;

use super::lock_table::LockTable;
use crate::error::DbResult;
use crate::storage::BlockId;
use crate::tx::concurrency::LockType;

/// Concurrency manager which maintains all locks held by transactions.
/// Interrior mutable.
pub struct ConcurrencyMgr {
    pub lock_table: Arc<LockTable>,
    locks: HashMap<BlockId, LockType>,
}

impl ConcurrencyMgr {
    pub fn new(lock_table: Arc<LockTable>) -> Self {
        Self {
            lock_table: lock_table,
            locks: HashMap::new(),
        }
    }

    pub fn lock_s(&mut self, blk: &BlockId, tx_id: i32) -> DbResult<()> {
        if !self.locks.contains_key(blk) {
            self.lock_table.lock_s(blk, tx_id)?;
            self.locks.insert(blk.clone(), LockType::Shared);
        }
        Ok(())
    }

    pub fn lock_x(&mut self, blk: &BlockId, tx_id: i32) -> DbResult<()> {
        let current_lock = self.locks.get(&blk);
        match current_lock {
            Some(LockType::Exclusive) => {
                return Ok(());
            },
            Some(LockType::Shared) => {
                self.lock_table.upgrade_to_x(blk, tx_id)?;
                self.locks.insert(blk.clone(), LockType::Exclusive);
            },
            None => {
                self.lock_table.lock_x(blk, tx_id)?;
                self.locks.insert(blk.clone(), LockType::Exclusive);
            },
        }
        Ok(())
    }

    pub fn release(&mut self, tx_id: i32) {
        for blk in self.locks.keys() {
            self.lock_table.unlock(blk, tx_id);
        }
        self.locks.clear();
    }

    fn has_exclusive_lock(&self, blk: &BlockId) -> bool {
        matches!(self.locks.get(blk), Some(LockType::Exclusive))
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use crate::error::DbError;

    use super::*;
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn test_concurrency_mgr() -> DbResult<()> {
        let lock_table = Arc::new(LockTable::new());
        let mut ccy_mgr = ConcurrencyMgr::new(Arc::clone(&lock_table));
        let blk = BlockId::new("testfile".to_string(), 1);

        ccy_mgr.lock_s(&blk,0)?;

        let lock_table_clone = Arc::clone(&lock_table);
        let blk_clone = blk.clone();

        let handle = thread::spawn(move || -> DbResult<()> {
            let mut cm2: ConcurrencyMgr = ConcurrencyMgr::new(lock_table_clone);
            cm2.lock_s(&blk_clone, 1)?;

            let result = cm2.lock_x(&blk_clone, 1);

            // two S locks - fail to acquire X lock
            assert!(matches!(result, Err(DbError::LockAbort)));

            cm2.release(1);
            Ok(())
        });

        handle.join().unwrap()?;

        // single S lock now - should acquire X lock
        let result = ccy_mgr.lock_x(&blk, 0);
        assert!(result.is_ok());

        ccy_mgr.release(0);

        let lock_table_clone = Arc::clone(&lock_table);
        let blk_clone = blk.clone();

        let handle = thread::spawn(move || -> DbResult<()> {
            // it's free, should be able to acquire X lock
            let mut cm3 = ConcurrencyMgr::new(lock_table_clone);
            cm3.lock_x(&blk_clone, 2)?;
            cm3.release(2);
            Ok(())
        });

        handle.join().unwrap()?;

        Ok(())
    }

    #[test]
    fn test_concurrency_mgr_stress_exclusive_locks() -> DbResult<()> {
        const NUM_THREADS: usize = 4;
        const OPERATIONS_PER_THREAD: usize = 10_000;
        const NUM_BLOCKS: usize = 3;
        
        let lock_table = Arc::new(LockTable::new());
        let start_time = Instant::now();
        
        let mut handles = Vec::new();
        for thread_id in 0..NUM_THREADS {
            let lock_table_clone = Arc::clone(&lock_table);
            
            let handle = thread::spawn(move || -> DbResult<()> {
                let mut rng = rand::rng();
                let mut concurrency_mgr = ConcurrencyMgr::new(Arc::clone(&lock_table_clone));
                let mut total_lock_time = Duration::ZERO;
                
                for _ in 0..OPERATIONS_PER_THREAD {
                    let block_num = rng.random_range(0..NUM_BLOCKS);
                    let blk = BlockId::new(format!("stress_file"), block_num as i32);
                    let lock_start = Instant::now();
                    
                    concurrency_mgr.lock_x(&blk, 0).unwrap();
                    thread::sleep(Duration::from_micros(1));
                    concurrency_mgr.release(0);
                    // concurrency_mgr = ConcurrencyMgr::new(Arc::clone(&lock_table_clone));
                    
                    total_lock_time += lock_start.elapsed();
                }
                
                println!("Thread {} average lock time: {:?}", 
                        thread_id, total_lock_time / OPERATIONS_PER_THREAD as u32);
                
                Ok(())
            });
            
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap()?;
        }
        
        let total_time = start_time.elapsed();
        let total_operations = NUM_THREADS * OPERATIONS_PER_THREAD;
        
        println!("\nStress test completed in {:.2} seconds", total_time.as_secs_f64());
        println!("Total operations: {}", total_operations);
        println!("Operations per second: {:.2}", 
                total_operations as f64 / total_time.as_secs_f64());
        println!("Average time per operation: {:.2} microseconds", 
                total_time.as_micros() as f64 / total_operations as f64);
        
        Ok(())
    }

    #[test]
    fn test_concurrency_mgr_mixed_lock_patterns() -> DbResult<()> {
        const NUM_THREADS: usize = 4;
        const OPERATIONS_PER_THREAD: usize = 5_000;
        const NUM_BLOCKS: usize = 3;
        
        println!("Starting mixed lock pattern test: {} threads, {} operations each", 
                NUM_THREADS, OPERATIONS_PER_THREAD);
        
        let lock_table = Arc::new(LockTable::new());
        let start_time = Instant::now();
        
        let mut handles = Vec::new();
        for thread_id in 0..NUM_THREADS {
            let lock_table_clone = Arc::clone(&lock_table);
            
            let handle = thread::spawn(move || -> DbResult<()> {
                let mut rng = rand::rng();
                let mut concurrency_mgr = ConcurrencyMgr::new(Arc::clone(&lock_table_clone));
                let mut exclusive_locks = 0;
                let mut shared_locks = 0;
                let mut failed_locks = 0;
                
                for op_num in 0..OPERATIONS_PER_THREAD {
                    let block_num = rng.random_range(0..NUM_BLOCKS);
                    let blk = BlockId::new(format!("mixed_file_{}", thread_id), block_num as i32);
                    
                    let use_exclusive = op_num % 3 == 0;
                    
                    let result = if use_exclusive {
                        concurrency_mgr.lock_x(&blk, op_num as i32)
                    } else {
                        concurrency_mgr.lock_s(&blk, op_num as i32)
                    };
                    
                    match result {
                        Ok(()) => {
                            if use_exclusive {
                                exclusive_locks += 1;
                            } else {
                                shared_locks += 1;
                            }
                            
                            thread::sleep(Duration::from_micros(2));
                            
                            concurrency_mgr.release(op_num as i32);
                            concurrency_mgr = ConcurrencyMgr::new(Arc::clone(&lock_table_clone));
                        }
                        Err(e) => {
                            failed_locks += 1;
                            println!("Thread {}: Operation {} failed: {:?}", thread_id, op_num, e);
                        }
                    }
                    
                    if op_num > 0 && op_num % 1000 == 0 {
                        println!("Thread {}: {} operations completed", thread_id, op_num);
                    }
                }
                
                println!("Thread {}: {} exclusive, {} shared, {} failed", 
                        thread_id, exclusive_locks, shared_locks, failed_locks);
                
                Ok(())
            });
            
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap()?;
        }
        
        let total_time = start_time.elapsed();
        println!("Mixed lock test completed in {:.2} seconds", total_time.as_secs_f64());
        
        Ok(())
    }
}
