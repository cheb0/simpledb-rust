pub struct CheckpointRecord {}

impl CheckpointRecord {
    pub fn new() -> Self {
        CheckpointRecord {}
    }
    
    pub fn write_to_page(&self, page: &mut Page) {
        page.set_int(0, CHECKPOINT);
    }
}

impl LogRecord for CheckpointRecord {
    fn op(&self) -> i32 {
        CHECKPOINT
    }

    fn tx_number(&self) -> i32 {
        -1
    }

    fn undo(&self, _tx_num: i32, _buffer_mgr: &Arc<BufferMgr>) -> DbResult<()> {
        Ok(())
    }
}