use std::any::Any;

use bincode::serialize;
use serde::{Deserialize, Serialize};

use crate::{error::DbResult, storage::Page, tx::Transaction};

use super::log_record::{LogRecord, ROLLBACK_FLAG};

#[derive(Serialize, Deserialize)]
pub struct RollbackRecord {
    tx_num: i32,
}

impl RollbackRecord {
    pub fn new(page: &Page) -> Self {
        RollbackRecord {
            tx_num: page.get_int(4),
        }
    }

    pub fn create(tx_num: i32) -> Self {
        RollbackRecord { tx_num }
    }

    pub fn to_bytes(&self) -> DbResult<Vec<u8>> {
        let mut result = vec![ROLLBACK_FLAG as u8];
        result.extend(serialize(self)?);
        Ok(result)
    }
}

impl LogRecord for RollbackRecord {
    fn op(&self) -> i32 {
        ROLLBACK_FLAG
    }

    fn tx_number(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, _tx_num: i32, _tx: Transaction) -> DbResult<()> {
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

    #[test]
    fn test_rollback_record_serialization() -> crate::error::DbResult<()> {
        let record = RollbackRecord::create(42);
        let bytes = record.to_bytes()?;
        
        let deserialized = create_log_record(&bytes)?;
        
        assert_eq!(deserialized.op(), ROLLBACK_FLAG);
        assert_eq!(deserialized.tx_number(), 42);
        
        let rollback = (&*deserialized).as_any().downcast_ref::<RollbackRecord>()
            .expect("Failed to downcast to RollbackRecord");
        assert_eq!(rollback.tx_num, 42);
        Ok(())
    }
}