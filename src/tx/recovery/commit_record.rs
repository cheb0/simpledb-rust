/// A commit transaction log record.
pub struct CommitRecord {
    tx_num: i32,
}

impl CommitRecord {
    pub fn new(page: &Page) -> Self {
        CommitRecord {
            tx_num: page.get_int(4),
        }
    }

    pub fn create(tx_num: i32) -> Self {
        CommitRecord { tx_num }
    }
    
    pub fn write_to_page(&self, page: &mut Page) {
        page.set_int(0, COMMIT);
        page.set_int(4, self.tx_num);
    }
}

impl LogRecord for CommitRecord {
    fn op(&self) -> i32 {
        COMMIT
    }

    fn tx_number(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, _tx_num: i32, _buffer_mgr: &Arc<BufferMgr>) -> DbResult<()> {
        // Commit records don't need to be undone
        Ok(())
    }
}