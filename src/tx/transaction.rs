use std::{cell::RefCell, rc::Rc, sync::{atomic::{AtomicI32, Ordering}, Arc}};
use crate::{error::DbError, storage::{BlockId, FileMgr}};
use crate::buffer::{BufferMgr, BufferList};
use crate::log::LogMgr;
use crate::error::DbResult;

use super::recovery::{commit_record::CommitRecord, log_record::{create_log_record, START_FLAG}, rollback_record::RollbackRecord, set_int_record::SetIntRecord, set_string_record::SetStringRecord, start_record::StartRecord};

static NEXT_TX_NUM: AtomicI32 = AtomicI32::new(0);

pub struct TransactionInner<'a> {
    buffer_mgr: &'a BufferMgr,
    log_mgr: Arc<LogMgr>,
    file_mgr: Arc<FileMgr>,
    tx_num: i32,
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
    ) -> DbResult<Self> {
        let tx_num = NEXT_TX_NUM.fetch_add(1, Ordering::SeqCst) + 1;
        
        // Create start record and log it
        let start_record = StartRecord::create(tx_num);
        let bytes = start_record.to_bytes()?;
        log_mgr.append(&bytes)?;
        
        let buffers = BufferList::new(&buffer_mgr);
        
        let inner = TransactionInner {
            buffer_mgr,
            log_mgr,
            file_mgr,
            tx_num,
            buffers,
        };
        
        Ok(Transaction {
            inner: Rc::new(RefCell::new(inner)),
        })
    }

    pub fn commit(&self) -> DbResult<()> {
        let mut inner = self.inner.borrow_mut();
        inner.buffer_mgr.flush_all(inner.tx_num)?;
        
        let commit_record = CommitRecord::new(inner.tx_num);
        let bytes = commit_record.to_bytes()?;
        let lsn = inner.log_mgr.append(&bytes)?;
        inner.log_mgr.flush(lsn)?;
        
        inner.buffers.unpin_all();
        Ok(())
    }

    pub fn rollback(&self) -> DbResult<()> {
        self.do_rollback()?;
        
        let mut inner = self.inner.borrow_mut();
        inner.buffer_mgr.flush_all(inner.tx_num)?;
        
        let rollback_record = RollbackRecord::create(inner.tx_num);
        let bytes = rollback_record.to_bytes()?;
        let lsn = inner.log_mgr.append(&bytes)?;
        inner.log_mgr.flush(lsn)?;
        
        inner.buffers.unpin_all();
        Ok(())
    }

    fn do_rollback(&self) -> DbResult<()> {
        let inner = self.inner.borrow();
        let log_mgr = Arc::clone(&inner.log_mgr); // TODO
        let tx_num = inner.tx_num;
        drop(inner);
        
        let mut iter = log_mgr.iterator()?;
        
        while let bytes = iter.next()? {
            let record = create_log_record(&bytes)?;
            
            if record.tx_number() == tx_num {
                if record.op() == START_FLAG {
                    return Ok(());
                }
                record.undo(tx_num, self.clone())?;
            }
        }
        
        Ok(())
    }

    pub fn pin(&self, blk: &BlockId) -> DbResult<()> {
        self.inner.borrow_mut().buffers.pin(blk)
    }

    pub fn unpin(&self, blk: &BlockId) {
        self.inner.borrow_mut().buffers.unpin(blk);
    }

    pub fn get_int(&self, blk: &BlockId, offset: usize) -> DbResult<i32> {
        let inner = self.inner.borrow();
        let guard = inner.buffers.get_buffer(blk)
            .ok_or_else(|| DbError::BufferNotFound(blk.clone()))?;
        let buffer = guard.borrow();
        Ok(buffer.contents().get_int(offset))
    }

    pub fn get_string(&self, blk: &BlockId, offset: usize) -> DbResult<String> {
        let inner = self.inner.borrow();
        let guard = inner.buffers.get_buffer(blk)
            .ok_or_else(|| DbError::BufferNotFound(blk.clone()))?;
        let buffer = guard.borrow();
        Ok(buffer.contents().get_string(offset))
    }

    pub fn set_int(&self, blk: &BlockId, offset: usize, val: i32, log: bool/*TODO should be true by default*/) -> DbResult<()> {
        let inner = self.inner.borrow();
        let guard = inner.buffers.get_buffer(blk)
            .ok_or_else(|| DbError::BufferNotFound(blk.clone()))?;
        let mut buffer = guard.borrow_mut();
        
        if log {
            let old_val = buffer.contents().get_int(offset);
            let blk_clone = buffer.block().expect("Buffer has no block assigned").clone();
            
            let set_int_record = SetIntRecord::new(inner.tx_num, blk_clone, offset, old_val);
            let bytes = set_int_record.to_bytes()?;
            let lsn = inner.log_mgr.append(&bytes)?;
            
            buffer.set_modified(inner.tx_num, lsn);
        }
        
        buffer.contents_mut().set_int(offset, val);
        Ok(())
    }

    pub fn set_string(&self, blk: &BlockId, offset: usize, val: &str, log: bool) -> DbResult<()> {
        let inner = self.inner.borrow();
        let guard = inner.buffers.get_buffer(blk)
            .ok_or_else(|| DbError::BufferNotFound(blk.clone()))?;
        let mut buffer = guard.borrow_mut();
        
        if log {
            let old_val = buffer.contents().get_string(offset);
            let blk_clone = buffer.block().expect("Buffer has no block assigned").clone();
            
            let set_string_record = SetStringRecord::new(inner.tx_num, blk_clone, offset, old_val);
            let bytes = set_string_record.to_bytes()?;
            let lsn = inner.log_mgr.append(&bytes)?;
            
            buffer.set_modified(inner.tx_num, lsn);
        }
        
        buffer.contents_mut().set_string(offset, val);
        Ok(())
    }

    pub fn size(&self, file_name: &str) -> DbResult<i32> {
        let inner = self.inner.borrow();
        Ok(inner.file_mgr.block_count(file_name)?)
    }

    pub fn append(&self, file_name: &str) -> DbResult<BlockId> {
        let inner = self.inner.borrow();
        Ok(inner.file_mgr.append(file_name)?)
    }

    pub fn block_size(&self) -> usize {
        self.inner.borrow().file_mgr.block_size()
    }

    pub fn available_buffs(&self) -> usize {
        self.inner.borrow().buffer_mgr.available()
    }
    
    pub fn tx_num(&self) -> i32 {
        self.inner.borrow().tx_num
    }
}

impl<'a> Clone for Transaction<'a> {
    fn clone(&self) -> Self {
        Self { inner: Rc::clone(&self.inner) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

     #[test]
    fn test_transaction_basic() -> DbResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_mgr = Arc::new(FileMgr::new(temp_dir.path(), 400)?);
        let log_mgr = Arc::new(LogMgr::new(Arc::clone(&file_mgr), "testlog")?);
        let buffer_mgr = Arc::new(BufferMgr::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), 3));
        let tx: Transaction<'_> = Transaction::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), &buffer_mgr)?;
        
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
        let temp_dir = TempDir::new().unwrap();
        let file_mgr = Arc::new(FileMgr::new(temp_dir.path(), 400)?);
        let log_mgr = Arc::new(LogMgr::new(Arc::clone(&file_mgr), "testlog")?);
        let buffer_mgr = Arc::new(BufferMgr::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), 3));
        
        let tx1: Transaction<'_> = Transaction::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), &buffer_mgr)?;

        let blk1 = tx1.append("testfile")?;

        tx1.pin(&blk1)?;
        tx1.set_int(&blk1, 50, 777, true)?;
        tx1.set_string(&blk1, 200, "ABC", true)?;
        
        tx1.commit()?;

        let tx2: Transaction<'_> = Transaction::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), &buffer_mgr)?;
        tx2.pin(&blk1)?;

        let int_val = tx2.get_int(&blk1, 50)?;
        assert_eq!(int_val, 777);
        let str_val = tx2.get_string(&blk1, 200)?;
        assert_eq!(str_val, "ABC");

        tx2.set_int(&blk1, 50, 999, true)?;
        tx2.set_string(&blk1, 200, "CDE", true)?;
        tx2.rollback()?;

        let tx3: Transaction<'_> = Transaction::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), &buffer_mgr)?;
        tx3.pin(&blk1)?;

        let int_val2 = tx3.get_int(&blk1, 50)?;
        assert_eq!(int_val2, 777);


        Ok(())
    }

    #[test]
    fn test_transaction_rollback() -> DbResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_mgr = Arc::new(FileMgr::new(temp_dir.path(), 400)?);
        let log_mgr = Arc::new(LogMgr::new(Arc::clone(&file_mgr), "testlog")?);
        let buffer_mgr = Arc::new(BufferMgr::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), 3));
        
        let tx1: Transaction<'_> = Transaction::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), &buffer_mgr)?;

        let blk1 = tx1.append("testfile")?;

        tx1.pin(&blk1)?;
        tx1.set_int(&blk1, 50, 777, true)?;
        tx1.set_int(&blk1, 200, 123, true)?;
        tx1.set_string(&blk1, 300, "ABC", true)?;
        
        tx1.commit()?;

        let tx2: Transaction<'_> = Transaction::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), &buffer_mgr)?;
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

        let tx3: Transaction<'_> = Transaction::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), &buffer_mgr)?;
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