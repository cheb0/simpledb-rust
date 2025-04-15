use serde::{Serialize, Deserialize};

/// Represents a block identifier in the SimpleDB system.
/// Each block is identified by a filename and a block number.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlockId {
    filename: String,
    blknum: i32,
}

impl BlockId {
    pub fn new(filename: String, blknum: i32) -> Self {
        BlockId { filename, blknum }
    }
    
    pub fn filename(&self) -> &str {
        &self.filename
    }
    
    pub fn number(&self) -> i32 {
        self.blknum
    }
} 