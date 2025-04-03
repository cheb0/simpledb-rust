/// Represents a block identifier in the SimpleDB system.
/// Each block is identified by a filename and a block number.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockId {
    filename: String,
    blknum: i32,
}

impl BlockId {
    /// Creates a new BlockId with the specified filename and block number.
    pub fn new(filename: String, blknum: i32) -> Self {
        BlockId { filename, blknum }
    }
    
    /// Returns the filename associated with this block.
    pub fn filename(&self) -> &str {
        &self.filename
    }
    
    /// Returns the block number.
    pub fn number(&self) -> i32 {
        self.blknum
    }
} 