use std::sync::Arc;

use crate::buffer::buffer::Buffer;
use crate::buffer::buffer_mgr::BufferMgr;
use crate::error::DbResult;
use crate::log::LogMgr;
use super::set_int_record::SetIntRecord;
use super::set_string_record::SetStringRecord;

/// Manages the recovery process for a transaction.
pub struct RecoveryMgr<'a> {
    log_mgr: Arc<LogMgr>,
    buffer_mgr: &'a BufferMgr,
    tx_num: i32,
}

impl<'a> RecoveryMgr<'a> {
    pub fn new(tx_num: i32, log_mgr: Arc<LogMgr>, buffer_mgr: &'a BufferMgr) -> DbResult<Self> {

        Ok(RecoveryMgr {
            log_mgr,
            buffer_mgr,
            tx_num,
        })
    }    
    
    /// Logs a modification to an integer value in a buffer.
    pub fn set_int(&self, buffer: &mut Buffer, offset: usize, new_val: i32) -> DbResult<i32> {
        let old_val = buffer.contents().get_int(offset);
        let blk = buffer.block().expect("Buffer has no block assigned"); // TODO avoid panic
        
        let set_int_record = SetIntRecord::new(self.tx_num, blk.clone() /* TODO do something with this */, offset, old_val);
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