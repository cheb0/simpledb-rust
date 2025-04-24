use std::sync::{Arc, atomic::{AtomicI32, Ordering}};
use crate::{buffer::buffer::Buffer, error::DbError, storage::{block_id::BlockId, file_mgr::FileMgr}};
use crate::buffer::{buffer_mgr::BufferMgr, buffer_list::BufferList};
use crate::log::LogMgr;
use crate::error::DbResult;

use super::recovery::{commit_record::CommitRecord, log_record::{create_log_record, START_FLAG}, rollback_record::RollbackRecord, set_int_record::SetIntRecord, set_string_record::SetStringRecord, start_record::StartRecord};

static NEXT_TX_NUM: AtomicI32 = AtomicI32::new(0);
const END_OF_FILE: i32 = -1;

pub struct Transaction<'a> {
    buffer_mgr: &'a BufferMgr,
    log_mgr: Arc<LogMgr>,
    file_mgr: Arc<FileMgr>,
    tx_num: i32,
    buffers: BufferList<'a>,
}

impl<'a> Transaction<'a> {
    pub fn new(
        file_mgr: Arc<FileMgr>,
        log_mgr: Arc<LogMgr>,
        buffer_mgr: &'a BufferMgr,
    ) -> DbResult<Self> {
        let tx_num = NEXT_TX_NUM.fetch_add(1, Ordering::SeqCst) + 1;

        let start_record = StartRecord::create(tx_num);
        let bytes = start_record.to_bytes()?;
        log_mgr.append(&bytes)?;

        let buffers = BufferList::new(&buffer_mgr);

        let tx = Transaction {
            buffer_mgr,
            log_mgr,
            file_mgr,
            tx_num,
            buffers,
        };
        Ok(tx)
    }

    pub fn commit(&mut self) -> DbResult<()> {
        self.buffer_mgr.flush_all(self.tx_num)?;
        
        let commit_record = CommitRecord::new(self.tx_num);
        let bytes = commit_record.to_bytes()?;
        let lsn = self.log_mgr.append(&bytes)?;
        self.log_mgr.flush(lsn)?;

        self.buffers.unpin_all();
        Ok(())
    }

    pub fn rollback(&mut self) -> DbResult<()> {
        self.do_rollback()?;

        self.buffer_mgr.flush_all(self.tx_num)?;

        let rollback_record = RollbackRecord::create(self.tx_num);
        let bytes = rollback_record.to_bytes()?;
        let lsn = self.log_mgr.append(&bytes)?;
        self.log_mgr.flush(lsn)?;

        self.buffers.unpin_all();
        Ok(())
    }

    fn do_rollback(&mut self) -> DbResult<()> {
        let log_mgr = Arc::clone(&self.log_mgr); // TODO this is a workaround for borrow checker, should fix
        let mut iter: crate::log::LogIterator<'_> = log_mgr.iterator()?;
        
        while let bytes = iter.next()? {
            let record = create_log_record(&bytes)?;
            
            if record.tx_number() == self.tx_num {
                if record.op() == START_FLAG {
                    return Ok(());
                }
                record.undo(self.tx_num, self)?;
            }
        }
        
        Ok(())
    }

    pub fn pin(&mut self, blk: &BlockId) -> DbResult<()> {
        self.buffers.pin(blk)
    }

    pub fn unpin(&mut self, blk: &BlockId) {
        self.buffers.unpin(blk);
    }

    pub fn get_int(&self, blk: &BlockId, offset: usize) -> DbResult<i32> {
        let guard = self.buffers.get_buffer(blk)
            .ok_or_else(|| DbError::BufferNotFound(blk.clone()))?;
        let buffer = guard.borrow();
        Ok(buffer.contents().get_int(offset))
    }

    pub fn get_string(&self, blk: &BlockId, offset: usize) -> DbResult<String> {
        let guard = self.buffers.get_buffer(blk)
            .ok_or_else(|| DbError::BufferNotFound(blk.clone()))?;
        let buffer = guard.borrow();
        Ok(buffer.contents().get_string(offset))
    }

    pub fn set_int(&self, blk: &BlockId, offset: usize, val: i32, log: bool) -> DbResult<()> {
        let guard = self.buffers.get_buffer(blk)
            .ok_or_else(|| DbError::BufferNotFound(blk.clone()))?;

        let mut buffer = guard.borrow_mut();
        
        if log {
            let lsn = self.log_set_int(&mut buffer, offset, val)?;
            buffer.set_modified(self.tx_num, lsn);
        }
        buffer.contents_mut().set_int(offset, val);
        
        Ok(())
    }

    fn log_set_int(&self, buffer: &mut Buffer, offset: usize, new_val: i32) -> DbResult<i32> {
        let old_val = buffer.contents().get_int(offset);
        let blk = buffer.block().expect("Buffer has no block assigned"); // TODO avoid panic
        
        let set_int_record: SetIntRecord = SetIntRecord::new(self.tx_num, blk.clone() /* TODO do something with this */, offset, old_val);
        let bytes = set_int_record.to_bytes()?;
        let lsn = self.log_mgr.append(&bytes)?;
        
        Ok(lsn)
    }

    pub fn set_string(&self, blk: &BlockId, offset: usize, val: String, log: bool) -> DbResult<()> {
        let guard = self.buffers.get_buffer(blk)
            .ok_or_else(|| DbError::BufferNotFound(blk.clone()))?;
        let mut buffer = guard.borrow_mut();
        
        if log {
            let lsn = self.log_set_string(&mut buffer, offset, &val)?;
            buffer.set_modified(self.tx_num, lsn);
        }

        buffer.contents_mut().set_string(offset, &val);
        Ok(())
    }

    fn log_set_string(&self, buffer: &mut Buffer, offset: usize, new_val: &str) -> DbResult<i32> {
        let old_val = buffer.contents().get_string(offset);
        let blk = buffer.block().expect("Buffer has no block assigned"); // TODO avoid panic
        
        let set_string_record: SetStringRecord = SetStringRecord::new(self.tx_num, blk.clone() /* TODO do something with this */, offset, old_val);
        let bytes = set_string_record.to_bytes()?;
        let lsn = self.log_mgr.append(&bytes)?;
        
        Ok(lsn)
    }

    pub fn size(&self, file_name: &str) -> DbResult<i32> {
        let dummy_blk = BlockId::new(file_name.to_string(), END_OF_FILE);
        Ok(self.file_mgr.block_count(file_name)?)
    }

    pub fn append(&self, file_name: &str) -> BlockId {
        let dummy_blk: BlockId = BlockId::new(file_name.to_string(), END_OF_FILE);
        self.file_mgr.append(file_name).unwrap()
    }

    pub fn block_size(&self) -> usize {
        self.file_mgr.block_size()
    }

    pub fn available_buffs(&self) -> usize {
        self.buffer_mgr.available()
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
        let mut tx: Transaction<'_> = Transaction::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), &buffer_mgr)?;
        
        let blk = tx.append("testfile");
        tx.pin(&blk)?;
        tx.set_int(&blk, 0, 123, true)?;
        tx.set_string(&blk, 100, "ABRACADABRA".to_string(), true)?;
        
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
        
        let mut tx1: Transaction<'_> = Transaction::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), &buffer_mgr)?;

        let blk1 = tx1.append("testfile");

        tx1.pin(&blk1)?;
        tx1.set_int(&blk1, 50, 777, true)?;
        tx1.set_string(&blk1, 200, "ABC".to_string(), true)?;
        
        tx1.commit()?;

        let mut tx2: Transaction<'_> = Transaction::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), &buffer_mgr)?;
        tx2.pin(&blk1)?;

        let int_val = tx2.get_int(&blk1, 50)?;
        assert_eq!(int_val, 777);
        let str_val = tx2.get_string(&blk1, 200)?;
        assert_eq!(str_val, "ABC");

        tx2.set_int(&blk1, 50, 999, true)?;
        tx2.set_string(&blk1, 200, "CDE".to_string(), true)?;
        tx2.rollback()?;

        let mut tx3: Transaction<'_> = Transaction::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), &buffer_mgr)?;
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
        
        let mut tx1: Transaction<'_> = Transaction::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), &buffer_mgr)?;

        let blk1 = tx1.append("testfile");

        tx1.pin(&blk1)?;
        tx1.set_int(&blk1, 50, 777, true)?;
        tx1.set_int(&blk1, 200, 123, true)?;
        tx1.set_string(&blk1, 300, "ABC".to_string(), true)?;
        
        tx1.commit()?;

        let mut tx2: Transaction<'_> = Transaction::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), &buffer_mgr)?;
        tx2.pin(&blk1)?;

        let value1 = tx2.get_int(&blk1, 50)?;
        assert_eq!(value1, 777);
        let value2 = tx2.get_int(&blk1, 200)?;
        assert_eq!(value2, 123);
        let str_val = tx2.get_string(&blk1, 300)?;
        assert_eq!(str_val, "ABC");

        tx2.set_int(&blk1, 50, 999, true)?;
        tx2.set_int(&blk1, 200, 234, true)?;
        tx2.set_string(&blk1, 300, "CDE".to_string(), true)?;
        tx2.rollback()?;

        let mut tx3: Transaction<'_> = Transaction::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), &buffer_mgr)?;
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