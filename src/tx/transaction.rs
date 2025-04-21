use std::sync::{Arc, atomic::{AtomicI32, Ordering}};
use crate::{error::DbError, storage::{block_id::BlockId, file_mgr::FileMgr}};
use crate::buffer::{buffer_mgr::BufferMgr, buffer_list::BufferList};
use crate::log::LogMgr;
use crate::error::DbResult;

use super::recovery::recovery_mgr::RecoveryMgr;

static NEXT_TX_NUM: AtomicI32 = AtomicI32::new(0);
const END_OF_FILE: i32 = -1;

pub struct Transaction<'a> {
    recovery_mgr: RecoveryMgr<'a>,
    buffer_mgr: &'a BufferMgr,
    file_mgr: Arc<FileMgr>,
    txnum: i32,
    buffers: BufferList<'a>,
}

impl<'a> Transaction<'a> {
    pub fn new(
        file_mgr: Arc<FileMgr>,
        log_mgr: Arc<LogMgr>,
        buffer_mgr: &'a BufferMgr,
    ) -> Self {
        let txnum = NEXT_TX_NUM.fetch_add(1, Ordering::SeqCst) + 1;
        let recovery_mgr = RecoveryMgr::new(
            txnum, 
            Arc::clone(&log_mgr), 
            buffer_mgr)
            .expect("fail"); // TODO
        let buffers = BufferList::new(&buffer_mgr);

        Transaction {
            recovery_mgr,
            buffer_mgr,
            file_mgr,
            txnum,
            buffers,
        }
    }

    pub fn commit(&mut self) {
        self.recovery_mgr.commit();
        self.buffers.unpin_all();
    }

    pub fn rollback(&mut self) {
        self.recovery_mgr.rollback();
        self.buffers.unpin_all();
    }

    pub fn pin(&mut self, blk: BlockId) -> DbResult<()> {
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
            let lsn = self.recovery_mgr.set_int(&mut buffer, offset, val)?;
            buffer.set_modified(self.txnum, lsn);
        }

        buffer.contents_mut().set_int(offset, val);
        
        Ok(())
    }

/*
    pub fn set_string(&self, blk: &BlockId, offset: usize, val: String, ok_to_log: bool) -> DbResult<()> {
        let guard = self.mybuffers.get_buffer(blk)
            .ok_or_else(|| DbError::General("Buffer not found".into()))?;
        let mut buffer = guard.borrow_mut();
        
        let lsn = if ok_to_log {
            self.recovery_mgr.set_string(&mut buffer, offset, &val)?
        } else {
            -1
        };

        buffer.contents_mut().set_string(offset, &val);
        buffer.set_modified(self.txnum, lsn);
        Ok(())
    }    
*/

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
        let mut tx = Transaction::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), &buffer_mgr);
        
        let blk = tx.append("testfile");
        tx.pin(blk.clone())?;
        tx.set_int(&blk, 0, 123, true)?;
        
        let val = tx.get_int(&blk, 0)?;
        assert_eq!(val, 123);
        
        tx.commit();
        Ok(())
    }
}