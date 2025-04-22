use thiserror::Error;
use bincode;

use crate::storage::block_id::BlockId;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("Buffer not found: {0:?}")]
    BufferNotFound(BlockId),

    #[error("Buffer abort exception: {0}")]
    BufferAbort(String),
    
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Transaction abort exception: {0}")]
    TransactionAbort(String),
    
    #[error("Deadlock exception")]
    Deadlock,

    #[error("Bad index value: {0}")]
    BadIndex(String),
    
    #[error("Schema exception: {0}")]
    Schema(String),
    
    #[error("Serialization error: {0}")]
    Serialization(String),
}

impl From<bincode::Error> for DbError {
    fn from(err: bincode::Error) -> Self {
        DbError::Serialization(err.to_string())
    }
}

pub type DbResult<T> = std::result::Result<T, DbError>;