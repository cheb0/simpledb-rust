pub struct StartRecord {
    tx_num: i32,
}

impl StartRecord {
    pub fn new(page: &Page) -> Self {
        StartRecord {
            tx_num: page.get_int(4),
        }
    }

    pub fn create(tx_num: i32) -> Self {
        StartRecord { tx_num }
    }
    
    pub fn write_to_page(&self, page: &mut Page) {
        page.set_int(0, START);
        page.set_int(4, self.tx_num);
    }
}

impl LogRecord for StartRecord {
    fn op(&self) -> i32 {
        START
    }

    fn tx_number(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, tx_num: i32, buffer_mgr: &Arc<BufferMgr>) -> DbResult<()> {
        // Start records don't need to be undone
        Ok(())
    }
}