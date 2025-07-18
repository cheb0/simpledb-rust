use std::any::Any;

use bincode::serialize;
use serde::{Deserialize, Serialize};

use crate::{error::DbResult, storage::Page, tx::Transaction};

use super::log_record::{LogRecord, START_FLAG};

#[derive(Serialize, Deserialize)]
pub struct StartRecord {
    tx_id: i32,
}

impl StartRecord {
    pub fn new(page: &Page) -> Self {
        StartRecord {
            tx_id: page.get_int(4),
        }
    }

    pub fn create(tx_id: i32) -> Self {
        StartRecord { tx_id }
    }

    pub fn to_bytes(&self) -> DbResult<Vec<u8>> {
        let mut result = vec![START_FLAG as u8];
        result.extend(serialize(self)?);
        Ok(result)
    }
}

impl LogRecord for StartRecord {
    fn op(&self) -> i32 {
        START_FLAG
    }

    fn tx_id(&self) -> i32 {
        self.tx_id
    }

    fn undo(&self, _tx_id: i32, _tx: Transaction) -> DbResult<()> {
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
    fn test_start_record_serialization() -> crate::error::DbResult<()> {
        let record = StartRecord::create(789);
        let bytes = record.to_bytes()?;
        
        let deserialized = create_log_record(&bytes)?;
        
        assert_eq!(deserialized.op(), START_FLAG);
        assert_eq!(deserialized.tx_id(), 789);
        
        let start = (&*deserialized).as_any().downcast_ref::<StartRecord>()
            .expect("Failed to downcast to StartRecord");
        assert_eq!(start.tx_id, 789);
        Ok(())
    }
}