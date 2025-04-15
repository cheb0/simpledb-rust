use std::{any::Any, sync::Arc};

use bincode::serialize;
use serde::{Deserialize, Serialize};

use crate::{buffer::buffer_mgr::BufferMgr, error::DbResult, storage::page::Page};

use super::log_record::{LogRecord, CHECKPOINT_FLAG};

#[derive(Serialize, Deserialize)]
pub struct CheckpointRecord {}

impl CheckpointRecord {
    pub fn new() -> Self {
        CheckpointRecord {}
    }

    pub fn to_bytes(&self) -> DbResult<Vec<u8>> {
        let mut result = vec![CHECKPOINT_FLAG as u8];
        result.extend(serialize(self)?);
        Ok(result)
    }
}

impl LogRecord for CheckpointRecord {
    fn op(&self) -> i32 {
        CHECKPOINT_FLAG
    }

    fn tx_number(&self) -> i32 {
        -1
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

    #[test]
    fn test_checkpoint_record_serialization() -> crate::error::DbResult<()> {
        let record = CheckpointRecord {};
        let bytes = record.to_bytes()?;
        
        let deserialized = create_log_record(&bytes)?;
        
        assert_eq!(deserialized.op(), CHECKPOINT_FLAG);
        assert_eq!(deserialized.tx_number(), -1);
        
        (&*deserialized).as_any().downcast_ref::<CheckpointRecord>()
            .expect("Failed to downcast to CheckpointRecord");

        Ok(())
    }
}