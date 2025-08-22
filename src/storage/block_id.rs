use serde::{Deserialize, Serialize};

/// Represents a block identifier in the SimpleDB system.
/// Each block is identified by a filename and a block number.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlockId {
    file_name: String,
    number: i32,
}

impl BlockId {
    pub fn new(file_name: String, num: i32) -> Self {
        BlockId {
            file_name,
            number: num,
        }
    }

    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    pub fn number(&self) -> i32 {
        self.number
    }
}
