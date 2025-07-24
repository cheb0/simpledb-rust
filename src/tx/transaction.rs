use std::{cell::RefCell, rc::Rc, sync::{atomic::{AtomicI32, Ordering}, Arc}};

use crate::{error::DbError, storage::{BlockId, FileMgr}, tx::concurrency::{ConcurrencyMgr, LockTable}};
use crate::buffer::{BufferMgr, BufferList};
use crate::log::LogMgr;
use crate::error::DbResult;

use super::recovery::{commit_record::CommitRecord, log_record::{create_log_record, START_FLAG}, rollback_record::RollbackRecord, set_int_record::SetIntRecord, set_string_record::SetStringRecord, start_record::StartRecord};

static NEXT_TX_ID: AtomicI32 = AtomicI32::new(0);

// Transaction is alive as long as DB is alive, so we can reference BufferMgr and other types.
pub struct TransactionInner<'a> {
    id: i32,
    buffer_mgr: &'a BufferMgr,
    concurrency_mgr: ConcurrencyMgr,
    log_mgr: Arc<LogMgr>,
    file_mgr: Arc<FileMgr>,
    buffers: BufferList<'a>,
}

pub struct Transaction<'a> {
    inner: Rc<RefCell<TransactionInner<'a>>>,
}

impl<'a> Transaction<'a> {
    pub fn new(
        file_mgr: Arc<FileMgr>,
        log_mgr: Arc<LogMgr>,
        buffer_mgr: &'a BufferMgr,
        lock_table: Arc<LockTable>,
    ) -> DbResult<Self> {
        let tx_id = NEXT_TX_ID.fetch_add(1, Ordering::SeqCst) + 1;

        let start_record = StartRecord::create(tx_id);
        let bytes = start_record.to_bytes()?;
        log_mgr.append(&bytes)?;
        
        let buffers = BufferList::new(&buffer_mgr);
        
        let inner = TransactionInner {
            buffer_mgr,
            log_mgr,
            file_mgr,
            id: tx_id,
            buffers,
            concurrency_mgr: ConcurrencyMgr::new(lock_table),
        };
        
        Ok(Transaction {
            inner: Rc::new(RefCell::new(inner)),
        })
    }

    pub fn commit(&self) -> DbResult<()> {
        let mut tx_inner = self.inner.borrow_mut();
        tx_inner.buffer_mgr.flush_all(tx_inner.id)?;
        
        let commit_record = CommitRecord::new(tx_inner.id);
        let bytes = commit_record.to_bytes()?;
        let lsn = tx_inner.log_mgr.append(&bytes)?;
        tx_inner.log_mgr.flush(lsn)?;
        // TODO fsync

        tx_inner.concurrency_mgr.release();
        
        tx_inner.buffers.unpin_all();
        Ok(())
    }

    pub fn rollback(&self) -> DbResult<()> {
        self.do_rollback()?;
        
        let mut tx_inner = self.inner.borrow_mut();
        tx_inner.buffer_mgr.flush_all(tx_inner.id)?;
        
        let rollback_record = RollbackRecord::create(tx_inner.id);
        let bytes = rollback_record.to_bytes()?;
        let lsn = tx_inner.log_mgr.append(&bytes)?;
        tx_inner.log_mgr.flush(lsn)?;
        
        tx_inner.concurrency_mgr.release();

        tx_inner.buffers.unpin_all();
        Ok(())
    }

    fn do_rollback(&self) -> DbResult<()> {
        let inner = self.inner.borrow();
        let log_mgr = Arc::clone(&inner.log_mgr); // TODO
        let tx_id = inner.id;
        drop(inner);
        
        let mut iter = log_mgr.iterator()?;
        
        while iter.has_next() {
            let bytes = iter.next()?;
            let record = create_log_record(&bytes)?;
            
            if record.tx_id() == tx_id {
                if record.op() == START_FLAG {
                    return Ok(());
                }
                record.undo(tx_id, self.clone())?;
            }
        }

        // we havent' seen a start record, the log is in inconsistent state
        Err(DbError::LogInconsistent)
    }

    pub fn pin(&self, blk: &BlockId) -> DbResult<()> {
        self.inner.borrow_mut().buffers.pin(blk)
    }

    pub fn unpin(&self, blk: &BlockId) {
        self.inner.borrow_mut().buffers.unpin(blk);
    }

    pub fn get_int(&self, blk: &BlockId, offset: usize) -> DbResult<i32> {
        let mut tx_inner = self.inner.borrow_mut();
        tx_inner.concurrency_mgr.lock_shared(blk)?;
        let guard = tx_inner.buffers.get_buffer(blk)
            .ok_or_else(|| DbError::BufferNotPinned(blk.clone()))?;
        let buffer = guard.borrow();
        Ok(buffer.page().get_int(offset))
    }

    pub fn get_string(&self, blk: &BlockId, offset: usize) -> DbResult<String> {
        let mut tx_inner = self.inner.borrow_mut();
        tx_inner.concurrency_mgr.lock_shared(blk)?;
        let guard = tx_inner.buffers.get_buffer(blk)
            .ok_or_else(|| DbError::BufferNotPinned(blk.clone()))?;
        let buffer = guard.borrow();
        Ok(buffer.page().get_string(offset))
    }

    pub fn set_int(&self, blk: &BlockId, offset: usize, val: i32, log: bool/*TODO should be true by default*/) -> DbResult<()> {
        let mut tx_inner = self.inner.borrow_mut();
        tx_inner.concurrency_mgr.lock_exclusive(blk)?;
        let guard = tx_inner.buffers.get_buffer(blk)
            .ok_or_else(|| DbError::BufferNotPinned(blk.clone()))?;
        let mut buffer = guard.borrow_mut();
        
        if log {
            let old_val = buffer.page().get_int(offset);
            let blk_clone = buffer.block().expect("Buffer has no block assigned").clone();
            
            let set_int_record = SetIntRecord::new(tx_inner.id, blk_clone, offset, old_val);
            let bytes = set_int_record.to_bytes()?;
            let lsn = tx_inner.log_mgr.append(&bytes)?;
            
            buffer.set_modified(tx_inner.id, lsn);
        }
        
        buffer.contents_mut().set_int(offset, val);
        Ok(())
    }

    pub fn set_string(&self, blk: &BlockId, offset: usize, val: &str, log: bool) -> DbResult<()> {
        let mut tx_inner = self.inner.borrow_mut();
        tx_inner.concurrency_mgr.lock_exclusive(blk)?;
        let guard = tx_inner.buffers.get_buffer(blk)
            .ok_or_else(|| DbError::BufferNotPinned(blk.clone()))?;
        let mut buffer = guard.borrow_mut();
        
        if log {
            let old_val = buffer.page().get_string(offset);
            let blk_clone = buffer.block().expect("Buffer has no block assigned").clone();
            
            let set_string_record = SetStringRecord::new(tx_inner.id, blk_clone, offset, old_val);
            let bytes = set_string_record.to_bytes()?;
            let lsn = tx_inner.log_mgr.append(&bytes)?;
            
            buffer.set_modified(tx_inner.id, lsn);
        }
        
        buffer.contents_mut().set_string(offset, val);
        Ok(())
    }

    pub fn size(&self, file_name: &str) -> DbResult<i32> {
        let mut tx_inner = self.inner.borrow_mut();
        let dummy_blk = BlockId::new(file_name.to_string(), -1);
        tx_inner.concurrency_mgr.lock_shared(&dummy_blk)?;
        Ok(tx_inner.file_mgr.block_cnt(file_name)?)
    }

    pub fn append(&self, file_name: &str) -> DbResult<BlockId> {
        let mut tx_inner = self.inner.borrow_mut();
        let dummy_blk = BlockId::new(file_name.to_string(), -1);
        tx_inner.concurrency_mgr.lock_exclusive(&dummy_blk)?;
        Ok(tx_inner.file_mgr.append(file_name)?)
    }

    pub fn block_size(&self) -> usize {
        self.inner.borrow().file_mgr.block_size()
    }

    pub fn available_buffs(&self) -> usize {
        self.inner.borrow().buffer_mgr.available()
    }
    
    pub fn id(&self) -> i32 {
        self.inner.borrow().id
    }
}

impl<'a> Clone for Transaction<'a> {
    fn clone(&self) -> Self {
        Self { inner: Rc::clone(&self.inner) }
    }
}

#[cfg(test)]
mod tests {
    use crate::tx::concurrency::lock_table::LockTable;
    use super::*;
    use tempfile::TempDir;

    struct TestEnvironment {
        _temp_dir: TempDir,
        file_mgr: Arc<FileMgr>,
        log_mgr: Arc<LogMgr>,
        buffer_mgr: Arc<BufferMgr>,
        lock_table: Arc<LockTable>,
    }

    impl TestEnvironment {
        fn new() -> DbResult<Self> {
            let temp_dir = TempDir::new().unwrap();
            let file_mgr = Arc::new(FileMgr::new(temp_dir.path(), 400)?);
            let log_mgr = Arc::new(LogMgr::new(Arc::clone(&file_mgr), "testlog")?);
            let buffer_mgr = Arc::new(BufferMgr::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), 3));
            let lock_table = Arc::new(LockTable::new());
            
            Ok(TestEnvironment {
                _temp_dir: temp_dir,
                file_mgr,
                log_mgr,
                buffer_mgr,
                lock_table,
            })
        }

        fn new_transaction(&self) -> DbResult<Transaction<'_>> {
            Transaction::new(
                Arc::clone(&self.file_mgr),
                Arc::clone(&self.log_mgr),
                &self.buffer_mgr,
                Arc::clone(&self.lock_table)
            )
        }
    }

    #[test]
    fn test_transaction_basic() -> DbResult<()> {
        let env = TestEnvironment::new()?;
        let tx = env.new_transaction()?;
        
        let blk = tx.append("testfile")?;
        tx.pin(&blk)?;
        tx.set_int(&blk, 0, 123, true)?;
        tx.set_string(&blk, 100, "ABRACADABRA", true)?;
        
        let int_val = tx.get_int(&blk, 0)?;
        assert_eq!(int_val, 123);
        let str_val = tx.get_string(&blk, 100)?;
        assert_eq!(str_val, "ABRACADABRA");
        
        tx.commit()?;
        Ok(())
    }

    #[test]
    fn test_transaction_rollback1() -> DbResult<()> {
        let env = TestEnvironment::new()?;
        
        let tx1 = env.new_transaction()?;
        let blk1 = tx1.append("testfile")?;

        tx1.pin(&blk1)?;
        tx1.set_int(&blk1, 50, 777, true)?;
        tx1.set_string(&blk1, 200, "ABC", true)?;
        
        tx1.commit()?;

        let tx2 = env.new_transaction()?;
        tx2.pin(&blk1)?;

        let int_val = tx2.get_int(&blk1, 50)?;
        assert_eq!(int_val, 777);
        let str_val = tx2.get_string(&blk1, 200)?;
        assert_eq!(str_val, "ABC");

        tx2.set_int(&blk1, 50, 999, true)?;
        tx2.set_string(&blk1, 200, "CDE", true)?;
        tx2.rollback()?;

        let tx3 = env.new_transaction()?;
        tx3.pin(&blk1)?;

        let int_val2 = tx3.get_int(&blk1, 50)?;
        assert_eq!(int_val2, 777);

        Ok(())
    }

    #[test]
    fn test_transaction_rollback() -> DbResult<()> {
        let env = TestEnvironment::new()?;
        
        let tx1 = env.new_transaction()?;
        let blk1 = tx1.append("testfile")?;

        tx1.pin(&blk1)?;
        tx1.set_int(&blk1, 50, 777, true)?;
        tx1.set_int(&blk1, 200, 123, true)?;
        tx1.set_string(&blk1, 300, "ABC", true)?;
        
        tx1.commit()?;

        let tx2 = env.new_transaction()?;
        tx2.pin(&blk1)?;

        let value1 = tx2.get_int(&blk1, 50)?;
        assert_eq!(value1, 777);
        let value2 = tx2.get_int(&blk1, 200)?;
        assert_eq!(value2, 123);
        let str_val = tx2.get_string(&blk1, 300)?;
        assert_eq!(str_val, "ABC");

        tx2.set_int(&blk1, 50, 999, true)?;
        tx2.set_int(&blk1, 200, 234, true)?;
        tx2.set_string(&blk1, 300, "CDE", true)?;
        tx2.rollback()?;

        let tx3 = env.new_transaction()?;
        tx3.pin(&blk1)?;

        let value1 = tx3.get_int(&blk1, 50)?;
        assert_eq!(value1, 777);
        let value2 = tx3.get_int(&blk1, 200)?;
        assert_eq!(value2, 123);
        let str_val2 = tx3.get_string(&blk1, 300)?;
        assert_eq!(str_val2, "ABC");

        Ok(())
    }
}