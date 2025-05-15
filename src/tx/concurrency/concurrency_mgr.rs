use std::collections::HashMap;
use std::sync::Arc;

use crate::error::DbResult;
use crate::storage::BlockId;
use super::lock_table::LockTable;

#[derive(Debug, Clone, PartialEq)]
pub enum LockType {
    Shared,
    Exclusive,
}

pub struct ConcurrencyMgr {
    pub /*TODO*/ lock_table: Arc<LockTable>,
    locks: HashMap<BlockId, LockType>,
}

impl ConcurrencyMgr {
    pub fn new(lock_table: Arc<LockTable>) -> Self {
        Self {
            lock_table,
            locks: HashMap::new(),
        }
    }

    pub fn lock_shared(&mut self, blk: &BlockId) -> DbResult<()> {
        if !self.locks.contains_key(blk) {
            self.lock_table.lock_shared(blk)?;
            self.locks.insert(blk.clone(), LockType::Shared);
        }
        Ok(())
    }

    pub fn lock_exclusive(&mut self, blk: &BlockId) -> DbResult<()> {
        if !self.has_exclusive_lock(blk) {
            self.lock_shared(blk)?;
            self.lock_table.lock_exclusive(blk)?;
            self.locks.insert(blk.clone(), LockType::Exclusive);
        }
        Ok(())
    }

    pub fn release(&mut self) {
        for blk in self.locks.keys() {
            self.lock_table.unlock(blk);
        }
        self.locks.clear();
    }

    fn has_exclusive_lock(&self, blk: &BlockId) -> bool {
        matches!(self.locks.get(blk), Some(LockType::Exclusive))
    }
}

#[cfg(test)]
mod tests {
    use crate::error::DbError;

    use super::*;
    use std::thread;

    #[test]
    fn test_concurrency_mgr() -> DbResult<()> {
        let lock_table = Arc::new(LockTable::new());
        let mut ccy_mgr = ConcurrencyMgr::new(Arc::clone(&lock_table));
        let blk = BlockId::new("testfile".to_string(), 1);
        
        ccy_mgr.lock_shared(&blk)?;
        
        let lock_table_clone = Arc::clone(&lock_table);
        let blk_clone = blk.clone();
        
        let handle = thread::spawn(move || -> DbResult<()> {
            let mut cm2: ConcurrencyMgr = ConcurrencyMgr::new(lock_table_clone);
            cm2.lock_shared(&blk_clone)?;

            let result = cm2.lock_exclusive(&blk_clone);

            // two S locks - fail to acquire X lock
            assert!(matches!(result, Err(DbError::LockAbort)));
            
            cm2.release();
            Ok(())
        });
        
        handle.join().unwrap()?;
        
        // single S lock now - should acquire X lock
        let result = ccy_mgr.lock_exclusive(&blk);
        assert!(result.is_ok());

        ccy_mgr.release();
        
        let lock_table_clone = Arc::clone(&lock_table);
        let blk_clone = blk.clone();
        
        let handle = thread::spawn(move || -> DbResult<()> {
            // it's free, should be able to acquire X lock
            let mut cm3 = ConcurrencyMgr::new(lock_table_clone);
            cm3.lock_exclusive(&blk_clone)?;
            cm3.release();
            Ok(())
        });
        
        handle.join().unwrap()?;
        
        Ok(())
    }
} 