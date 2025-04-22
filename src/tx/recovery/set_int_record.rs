use std::any::Any;

use bincode::serialize;
use serde::{Deserialize, Serialize};

use crate::{error::DbResult, storage::block_id::BlockId, tx::transaction::Transaction};

use super::log_record::{LogRecord, SETINT_FLAG};

#[derive(Serialize, Deserialize)]
pub struct SetIntRecord {
    pub tx_num: i32,
    pub offset: usize,
    pub val: i32,
    pub blk: BlockId,
}

impl SetIntRecord {
    pub fn new(tx_num: i32, blk: BlockId, offset: usize, val: i32) -> Self {
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

    fn undo(&self, tx_num: i32, tx: &mut Transaction) -> DbResult<()> {
        tx.pin(&self.blk)?;
        tx.set_int(&self.blk, self.offset, self.val, false)?;
        tx.unpin(&self.blk);
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx::recovery::log_record::create_log_record;
    use crate::storage::block_id::BlockId;

    #[test]
    fn test_set_int_record_serialization() -> crate::error::DbResult<()> {
        let blk = BlockId::new("testfile".to_string(), 42);
        let record = SetIntRecord::new(101, blk, 16, 9999);
        let bytes = record.to_bytes()?;
        
        let deserialized = create_log_record(&bytes)?;
        
        assert_eq!(deserialized.op(), SETINT_FLAG);
        assert_eq!(deserialized.tx_number(), 101);
        
        let set_int = (&*deserialized).as_any().downcast_ref::<SetIntRecord>()
            .expect("Failed to downcast to SetIntRecord");
        assert_eq!(set_int.tx_num, 101);
        assert_eq!(set_int.offset, 16);
        assert_eq!(set_int.val, 9999);
        assert_eq!(set_int.blk.file_name(), "testfile");
        assert_eq!(set_int.blk.number(), 42);
        Ok(())
    }
}