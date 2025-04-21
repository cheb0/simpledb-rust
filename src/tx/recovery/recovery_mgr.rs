use std::sync::Arc;

use crate::buffer::buffer::Buffer;
use crate::buffer::buffer_mgr::BufferMgr;
use crate::error::DbResult;
use crate::log::LogMgr;
use crate::storage::block_id::BlockId;

use super::checkpoint_record::CheckpointRecord;
use super::commit_record::CommitRecord;
use super::rollback_record::RollbackRecord;
use super::set_int_record::SetIntRecord;
use super::set_string_record::SetStringRecord;
use super::start_record::StartRecord;

/// Manages the recovery process for a transaction.
pub struct RecoveryMgr<'a> {
    log_mgr: Arc<LogMgr>,
    buffer_mgr: &'a BufferMgr,
    tx_num: i32,
}

impl<'a> RecoveryMgr<'a> {
    pub fn new(tx_num: i32, log_mgr: Arc<LogMgr>, buffer_mgr: &'a BufferMgr) -> DbResult<Self> {
        let start_record = StartRecord::create(tx_num);
        let bytes = start_record.to_bytes()?;
        log_mgr.append(&bytes)?;
        
        Ok(RecoveryMgr {
            log_mgr,
            buffer_mgr,
            tx_num,
        })
    }
    
    /// Commits the current transaction.
    pub fn commit(&self) -> DbResult<()> {
/*         // Flush all modified buffers for this transaction
        self.buffer_mgr.flush_all(self.tx_num)?;
        
        let commit_record = CommitRecord::new(self.tx_num);
        let bytes = commit_record.to_bytes()?;
        let lsn = self.log_mgr.append(&bytes)?;
        
        // Flush the log to ensure the commit record is persisted
        self.log_mgr.flush(lsn)?; */
        Ok(())
    }
    
    /// Rolls back the current transaction.
    pub fn rollback(&self) -> DbResult<()> {
/*         self.buffer_mgr.flush_all(self.tx_num)?;
        
        let rollback_record = RollbackRecord::create(self.tx_num);
        let bytes = rollback_record.to_bytes()?;
        let lsn = self.log_mgr.append(&bytes)?;
        
        self.log_mgr.flush(lsn)?; */
        Ok(())
    }
    
    /// Recovers the database after a crash.
    pub fn recover(&self) -> DbResult<()> {
/*         // Flush all modified buffers for this transaction
        self.buffer_mgr.flush_all(self.tx_num)?;
        
        let checkpoint_record = CheckpointRecord {};
        let bytes = checkpoint_record.to_bytes()?;
        let lsn = self.log_mgr.append(&bytes)?;
        
        self.log_mgr.flush(lsn)?; */
        
        Ok(())
    }
    
    /// Logs a modification to an integer value in a buffer.
    pub fn set_int(&self, buffer: &mut Buffer, offset: usize, new_val: i32) -> DbResult<i32> {
        let old_val = buffer.contents().get_int(offset);
        let blk = buffer.block().expect("Buffer has no block assigned");
        
        let set_int_record = SetIntRecord::new(self.tx_num, blk.clone() /* TODO do something with this */, offset as i32, old_val);
        let bytes = set_int_record.to_bytes()?;
        let lsn = self.log_mgr.append(&bytes)?;
        
        Ok(lsn)
    }
    
    /// Logs a modification to a string value in a buffer.
    pub fn set_string(&self, buff: &mut Buffer, offset: usize, new_val: &str) -> DbResult<i32> {
        let old_val = buff.contents().get_string(offset);
        let blk = buff.block().expect("Buffer has no block assigned");
        
        let set_string_record = SetStringRecord::new(self.tx_num, blk.clone() /* TODO do something with this */, offset as i32, old_val);
        let bytes = set_string_record.to_bytes()?;
        let lsn = self.log_mgr.append(&bytes)?;
        
        Ok(lsn)
    }
}

/*
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::file_mgr::FileMgr;
    use tempfile::tempdir;
    
    #[test]
    fn test_recovery_mgr_basic() -> DbResult<()> {
        let temp_dir = tempdir()?;
        let file_mgr = Arc::new(FileMgr::new(temp_dir.path(), 400)?);
        let log_mgr = Arc::new(LogMgr::new(Arc::clone(&file_mgr), "testlog")?);
        let buffer_mgr = Arc::new(BufferMgr::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), 10));
        let tx_num = 1;

        let recovery_mgr = RecoveryMgr::new(tx_num, Arc::clone(&log_mgr), Arc::clone(&buffer_mgr))?;
        let num_blocks = 10;
        for _ in 0..num_blocks {
            fm.append("testfile")?;
        }

        recovery_mgr.commit()?;
        
        let tx_num = 2;
        let recovery_mgr = RecoveryMgr::new(tx_num, Arc::clone(&log_mgr), Arc::clone(&buffer_mgr))?;
        
        recovery_mgr.rollback()?;
        
        // Create another recovery manager
        let tx_num = 3;
        let recovery_mgr = RecoveryMgr::new(tx_num, Arc::clone(&log_mgr), Arc::clone(&buffer_mgr))?;
        
        // Test recover
        recovery_mgr.recover()?;
        
        let blk = BlockId::new("testfile".to_string(), 4);
        let mut buffer = buffer_mgr.pin(&blk)?;
        let lsn = recovery_mgr.set_int(&mut buffer, 0, 42)?;
        assert!(lsn > 0);
        
        // Test set_string
        let lsn = recovery_mgr.set_string(&mut buffer, 4, "test")?;
        assert!(lsn > 0);
        
        // Unpin the buffer
        buffer_mgr.unpin(buffer);
        
        Ok(())
    }
} 
*/
