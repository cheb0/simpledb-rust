use std::any::Any;

use bincode::serialize;
use serde::{Deserialize, Serialize};

use crate::{error::DbResult, storage::page::Page, tx::transaction::Transaction};

use super::log_record::{LogRecord, START_FLAG};

#[derive(Serialize, Deserialize)]
pub struct StartRecord {
    tx_num: i32,
}

impl StartRecord {
    pub fn new(page: &Page) -> Self {
        StartRecord {
            tx_num: page.get_int(4),
        }
    }

    pub fn create(tx_num: i32) -> Self {
        StartRecord { tx_num }
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

    fn tx_number(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, tx_num: i32, tx: &mut Transaction) -> DbResult<()> {
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
    fn test_start_record_serialization() -> crate::error::DbResult<()> {
        let record = StartRecord::create(789);
        let bytes = record.to_bytes()?;
        
        let deserialized = create_log_record(&bytes)?;
        
        assert_eq!(deserialized.op(), START_FLAG);
        assert_eq!(deserialized.tx_number(), 789);
        
        let start = (&*deserialized).as_any().downcast_ref::<StartRecord>()
            .expect("Failed to downcast to StartRecord");
        assert_eq!(start.tx_num, 789);
        Ok(())
    }
}