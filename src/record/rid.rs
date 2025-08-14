#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct RID {
    block_number: i32,
    slot: usize,
}

impl RID {
    pub fn new(block_number: i32, slot: usize) -> Self {
        RID { block_number, slot }
    }
    
    pub fn block_number(&self) -> i32 {
        self.block_number
    }
    
    pub fn slot(&self) -> usize {
        self.slot
    }
}