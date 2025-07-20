use thiserror::Error;
use bincode;

use crate::storage::BlockId;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("Buffer not pinned: {0:?}")]
    BufferNotPinned(BlockId),

    #[error("Buffer abort exception: {0}")]
    BufferAbort(String),
    
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Schema exception: {0}")]
    Schema(String),
    
    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("No available slot")]
    NoAvailableSlot,

    #[error("Field not found: {0}")]
    FieldNotFound(String),

    #[error("Lock abort")]
    LockAbort,
}

impl From<bincode::Error> for DbError {
    fn from(err: bincode::Error) -> Self {
        DbError::Serialization(err.to_string())
    }
}

pub type DbResult<T> = std::result::Result<T, DbError>;