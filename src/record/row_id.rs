pub struct RowId {
    block_number: i32,
    slot: usize,
}

impl RowId {
    pub fn new(block_number: i32, slot: usize) -> Self {
        RowId { block_number, slot }
    }
    
    pub fn block_number(&self) -> i32 {
        self.block_number
    }
    
    pub fn slot(&self) -> usize {
        self.slot
    }
}