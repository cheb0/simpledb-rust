use std::sync::Arc;
use std::any::Any;

use bincode::deserialize;

use crate::buffer::buffer_mgr::BufferMgr;
use crate::error::DbResult;

use super::checkpoint_record::CheckpointRecord;
use super::commit_record::CommitRecord;
use super::rollback_record::RollbackRecord;
use super::set_int_record::SetIntRecord;
use super::set_string_record::SetStringRecord;
use super::start_record::StartRecord;

pub const CHECKPOINT_FLAG: i32 = 0;
pub const START_FLAG: i32 = 1;
pub const COMMIT_FLAG: i32 = 2;
pub const ROLLBACK_FLAG: i32 = 3;
pub const SETINT_FLAG: i32 = 4;
pub const SETSTRING_FLAG: i32 = 5;

pub trait LogRecord: Send + Sync {

    fn op(&self) -> i32;

    fn tx_number(&self) -> i32;

    /// Undoes the operation encoded by this log record.
    fn undo(&self, tx_num: i32, buffer_mgr: &Arc<BufferMgr>) -> DbResult<()>;

    fn as_any(&self) -> &dyn Any;
}

/// Creates a log record from bytes.
pub fn create_log_record(bytes: &[u8]) -> DbResult<Box<dyn LogRecord>> {
    let record_flag = bytes[0] as i32;
    
    match record_flag {
        CHECKPOINT_FLAG => Ok(Box::new(deserialize::<CheckpointRecord>(&bytes[1..])?)),
        START_FLAG => Ok(Box::new(deserialize::<StartRecord>(&bytes[1..])?)),
        COMMIT_FLAG => Ok(Box::new(deserialize::<CommitRecord>(&bytes[1..])?)),
        ROLLBACK_FLAG => Ok(Box::new(deserialize::<RollbackRecord>(&bytes[1..])?)),
        SETINT_FLAG => Ok(Box::new(deserialize::<SetIntRecord>(&bytes[1..])?)),
        SETSTRING_FLAG => Ok(Box::new(deserialize::<SetStringRecord>(&bytes[1..])?)),
        _ => Err(crate::error::DbError::Schema(format!("Unknown log record type: {}", record_flag))),
    }
}