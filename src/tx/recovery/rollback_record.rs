/// A rollback transaction log record.
pub struct RollbackRecord {
    tx_num: i32,
}

impl RollbackRecord {
    pub fn new(page: &Page) -> Self {
        RollbackRecord {
            tx_num: page.get_int(4),
        }
    }

    pub fn create(tx_num: i32) -> Self {
        RollbackRecord { tx_num }
    }
    
    pub fn write_to_page(&self, page: &mut Page) {
        page.set_int(0, ROLLBACK);
        page.set_int(4, self.tx_num);
    }
}

impl LogRecord for RollbackRecord {
    fn op(&self) -> i32 {
        ROLLBACK
    }

    fn tx_number(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, _tx_num: i32, _buffer_mgr: &Arc<BufferMgr>) -> DbResult<()> {
        // Rollback records don't need to be undone
        Ok(())
    }
}