use thiserror::Error;

#[derive(Error, Debug)]
pub enum DbError {
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
}

pub type DbResult<T> = std::result::Result<T, DbError>; 