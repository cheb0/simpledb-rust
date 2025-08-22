use std::any::Any;

use bincode::serialize;
use serde::{Deserialize, Serialize};

use crate::{error::DbResult, storage::BlockId, tx::Transaction};

use super::log_record::{LogRecord, SETSTRING_FLAG};

#[derive(Serialize, Deserialize)]
pub struct SetStringRecord {
    tx_id: i32,
    offset: usize,
    val: String,
    blk: BlockId,
}

impl SetStringRecord {
    pub fn new(tx_id: i32, blk: BlockId, offset: usize, val: String) -> Self {
        SetStringRecord {
            tx_id,
            offset,
            val,
            blk,
        }
    }

    pub fn to_bytes(&self) -> DbResult<Vec<u8>> {
        let mut result = vec![SETSTRING_FLAG as u8];
        result.extend(serialize(self)?);
        Ok(result)
    }
}

impl LogRecord for SetStringRecord {
    fn op(&self) -> i32 {
        SETSTRING_FLAG
    }

    fn tx_id(&self) -> i32 {
        self.tx_id
    }

    fn undo(&self, _tx_id: i32, tx: Transaction) -> DbResult<()> {
        tx.pin(&self.blk)?;
        tx.set_string(&self.blk, self.offset, &self.val, false)?;
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
    use crate::storage::BlockId;
    use crate::tx::recovery::log_record::create_log_record;

    #[test]
    fn test_set_string_record_serialization() -> crate::error::DbResult<()> {
        let blk = BlockId::new("datafile".to_string(), 123);
        let test_string = "Hello, world!".to_string();
        let record = SetStringRecord::new(202, blk, 32, test_string.clone());
        let bytes = record.to_bytes()?;

        let deserialized = create_log_record(&bytes)?;

        assert_eq!(deserialized.op(), SETSTRING_FLAG);
        assert_eq!(deserialized.tx_id(), 202);

        let set_string = (&*deserialized)
            .as_any()
            .downcast_ref::<SetStringRecord>()
            .expect("Failed to downcast to SetStringRecord");

        assert_eq!(set_string.tx_id, 202);
        assert_eq!(set_string.offset, 32);
        assert_eq!(set_string.val, test_string);
        assert_eq!(set_string.blk.file_name(), "datafile");
        assert_eq!(set_string.blk.number(), 123);

        Ok(())
    }
}
