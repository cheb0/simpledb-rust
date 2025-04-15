use std::sync::Arc;
use std::any::Any;

use bincode::serialize;
use serde::{Deserialize, Serialize};

use crate::{buffer::buffer_mgr::BufferMgr, error::DbResult, storage::{block_id::BlockId, page::Page}};

use super::log_record::{LogRecord, SETINT_FLAG};

/// A set integer log record.
#[derive(Serialize, Deserialize)]
pub struct SetIntRecord {
    pub tx_num: i32,
    pub offset: i32,
    pub val: i32,
    pub blk: BlockId,
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

    pub fn create(tx_num: i32, blk: BlockId, offset: i32, val: i32) -> Self {
        SetIntRecord {
            tx_num,
            offset,
            val,
            blk,
        }
    }

    pub fn to_bytes(&self) -> DbResult<Vec<u8>> {
        let mut result = vec![SETINT_FLAG as u8];
        result.extend(serialize(self)?);
        Ok(result)
    }
}

impl LogRecord for SetIntRecord {
    fn op(&self) -> i32 {
        SETINT_FLAG
    }

    fn tx_number(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, tx_num: i32, buffer_mgr: &Arc<BufferMgr>) -> DbResult<()> {
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx::recovery::log_record::{create_log_record, LogRecord};
    use crate::storage::block_id::BlockId;

    #[test]
    fn test_set_int_record_serialization() -> crate::error::DbResult<()> {
        let blk = BlockId::new("testfile".to_string(), 42);
        let record = SetIntRecord::create(101, blk, 16, 9999);
        let bytes = record.to_bytes()?;
        
        let deserialized = create_log_record(&bytes)?;
        
        assert_eq!(deserialized.op(), SETINT_FLAG);
        assert_eq!(deserialized.tx_number(), 101);
        
        let set_int = (&*deserialized).as_any().downcast_ref::<SetIntRecord>()
            .expect("Failed to downcast to SetIntRecord");
        assert_eq!(set_int.tx_num, 101);
        assert_eq!(set_int.offset, 16);
        assert_eq!(set_int.val, 9999);
        assert_eq!(set_int.blk.filename(), "testfile");
        assert_eq!(set_int.blk.number(), 42);
        Ok(())
    }
}