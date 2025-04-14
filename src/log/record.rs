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

pub struct CheckpointRecord {}

impl LogRecord for CheckpointRecord {
    fn op(&self) -> i32 {
        CHECKPOINT
    }

    fn tx_number(&self) -> i32 {
        -1
    }

    fn undo(&self, _tx_num: i32, _buffer_mgr: &Arc<BufferMgr>) -> DbResult<()> {
        Ok(())
    }
}

pub struct StartRecord {
    tx_num: i32,
}

impl StartRecord {
    pub fn new(page: &Page) -> Self {
        StartRecord {
            tx_num: page.get_int(4),
        }
    }
}

impl LogRecord for StartRecord {
    fn op(&self) -> i32 {
        START
    }

    fn tx_number(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, _tx_num: i32, _buffer_mgr: &Arc<BufferMgr>) -> DbResult<()> {
        // Start records don't need to be undone
        Ok(())
    }
}

/// A commit transaction log record.
pub struct CommitRecord {
    tx_num: i32,
}

impl CommitRecord {
    pub fn new(page: &Page) -> Self {
        CommitRecord {
            tx_num: page.get_int(4),
        }
    }
}

impl LogRecord for CommitRecord {
    fn op(&self) -> i32 {
        COMMIT
    }

    fn tx_number(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, _tx_num: i32, _buffer_mgr: &Arc<BufferMgr>) -> DbResult<()> {
        // Commit records don't need to be undone
        Ok(())
    }
}

/// A rollback transaction log record.
pub struct RollbackRecord {
    tx_num: i32,
}

impl RollbackRecord {
    pub fn new(page: &Page) -> Self {
        RollbackRecord {
            tx_num: page.get_int(4),
        }
    }
}

impl LogRecord for RollbackRecord {
    fn op(&self) -> i32 {
        ROLLBACK
    }

    fn tx_number(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, _tx_num: i32, _buffer_mgr: &Arc<BufferMgr>) -> DbResult<()> {
        // Rollback records don't need to be undone
        Ok(())
    }
}

/// A set integer log record.
pub struct SetIntRecord {
    tx_num: i32,
    offset: i32,
    val: i32,
    blk: BlockId,
}

impl SetIntRecord {
    pub fn new(page: &Page) -> Self {
        let tx_num = page.get_int(4);
        let filename = page.get_string(8);
        let block_num = page.get_int(8 + Page::max_length(filename.len()));
        let offset = page.get_int(12 + Page::max_length(filename.len()));
        let val = page.get_int(16 + Page::max_length(filename.len()));
        
        SetIntRecord {
            tx_num,
            offset,
            val,
            blk: BlockId::new(filename, block_num),
        }
    }
}

impl LogRecord for SetIntRecord {
    fn op(&self) -> i32 {
        SETINT
    }

    fn tx_number(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, tx_num: i32, buffer_mgr: &Arc<BufferMgr>) -> DbResult<()> {
        Ok(())
    }
}

pub struct SetStringRecord {
    tx_num: i32,
    offset: i32,
    val: String,
    blk: BlockId,
}

impl SetStringRecord {
    pub fn new(page: &Page) -> Self {
        let tx_num = page.get_int(4);
        let filename = page.get_string(8);
        let block_num = page.get_int(8 + Page::max_length(filename.len()));
        let offset = page.get_int(12 + Page::max_length(filename.len()));
        let val = page.get_string(16 + Page::max_length(filename.len()));
        
        SetStringRecord {
            tx_num,
            offset,
            val,
            blk: BlockId::new(filename, block_num),
        }
    }
}

impl LogRecord for SetStringRecord {
    fn op(&self) -> i32 {
        SETSTRING
    }

    fn tx_number(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, tx_num: i32, buffer_mgr: &Arc<BufferMgr>) -> DbResult<()> {
        // TODO
        Ok(())
    }
} 

impl CheckpointRecord {
    pub fn new() -> Self {
        CheckpointRecord {}
    }
    
    pub fn write_to_page(&self, page: &mut Page) {
        page.set_int(0, CHECKPOINT);
    }
}

impl StartRecord {
    pub fn create(tx_num: i32) -> Self {
        StartRecord { tx_num }
    }
    
    pub fn write_to_page(&self, page: &mut Page) {
        page.set_int(0, START);
        page.set_int(4, self.tx_num);
    }
}


impl CommitRecord {
    pub fn create(tx_num: i32) -> Self {
        CommitRecord { tx_num }
    }
    
    pub fn write_to_page(&self, page: &mut Page) {
        page.set_int(0, COMMIT);
        page.set_int(4, self.tx_num);
    }
}

impl RollbackRecord {
    pub fn create(tx_num: i32) -> Self {
        RollbackRecord { tx_num }
    }
    
    pub fn write_to_page(&self, page: &mut Page) {
        page.set_int(0, ROLLBACK);
        page.set_int(4, self.tx_num);
    }
}

impl SetIntRecord {
    pub fn create(tx_num: i32, blk: BlockId, offset: i32, val: i32) -> Self {
        SetIntRecord {
            tx_num,
            offset,
            val,
            blk,
        }
    }
    
    pub fn write_to_page(&self, page: &mut Page) {
        page.set_int(0, SETINT);
        page.set_int(4, self.tx_num);
        page.set_string(8, &self.blk.filename());
        let pos = 8 + Page::max_length(self.blk.filename().len());
        page.set_int(pos, self.blk.number());
        page.set_int(pos + 4, self.offset);
        page.set_int(pos + 8, self.val);
    }
}

impl SetStringRecord {
    pub fn create(tx_num: i32, blk: BlockId, offset: i32, val: String) -> Self {
        SetStringRecord {
            tx_num,
            offset,
            val,
            blk,
        }
    }
    
    pub fn write_to_page(&self, page: &mut Page) {
        page.set_int(0, SETSTRING);
        page.set_int(4, self.tx_num);
        page.set_string(8, &self.blk.filename());
        let pos = 8 + Page::max_length(self.blk.filename().len());
        page.set_int(pos, self.blk.number());
        page.set_int(pos + 4, self.offset);
        page.set_string(pos + 8, &self.val);
    }
}