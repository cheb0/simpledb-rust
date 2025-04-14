use std::sync::Arc;

use crate::storage::page::Page;
use crate::buffer::buffer_mgr::BufferMgr;
use crate::storage::block_id::BlockId;
use crate::error::DbResult;

pub const CHECKPOINT: i32 = 0;
pub const START: i32 = 1;
pub const COMMIT: i32 = 2;
pub const ROLLBACK: i32 = 3;
pub const SETINT: i32 = 4;
pub const SETSTRING: i32 = 5;

// TODO use serde or other idiomatic serialization framework
pub trait LogRecord: Send + Sync {

    fn op(&self) -> i32;

    fn tx_number(&self) -> i32;

    /// Undoes the operation encoded by this log record.
    fn undo(&self, tx_num: i32, buffer_mgr: &Arc<BufferMgr>) -> DbResult<()>;
}

pub fn create_log_record(bytes: &[u8]) -> Box<dyn LogRecord> {
    let page = Page::from_slice(bytes);
    
    match page.get_int(0) {
        CHECKPOINT => Box::new(CheckpointRecord {}),
        START => Box::new(StartRecord::new(&page)),
        COMMIT => Box::new(CommitRecord::new(&page)),
        ROLLBACK => Box::new(RollbackRecord::new(&page)),
        SETINT => Box::new(SetIntRecord::new(&page)),
        SETSTRING => Box::new(SetStringRecord::new(&page)),
        _ => panic!("Unknown log record type"),
    }
}