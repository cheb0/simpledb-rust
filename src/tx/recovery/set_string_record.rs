pub struct SetStringRecord {
    tx_num: i32,
    offset: i32,
    val: String,
    blk: BlockId,
}

impl SetStringRecord {
    pub fn new(page: &Page) -> Self {
        let tx_num = page.get_int(4);
        let filename = page.get_string(8);
        let block_num = page.get_int(8 + Page::max_length(filename.len()));
        let offset = page.get_int(12 + Page::max_length(filename.len()));
        let val = page.get_string(16 + Page::max_length(filename.len()));
        
        SetStringRecord {
            tx_num,
            offset,
            val,
            blk: BlockId::new(filename, block_num),
        }
    }
}

impl LogRecord for SetStringRecord {
    fn op(&self) -> i32 {
        SETSTRING
    }

    fn tx_number(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, tx_num: i32, buffer_mgr: &Arc<BufferMgr>) -> DbResult<()> {
        // TODO
        Ok(())
    }
}

impl SetStringRecord {
    pub fn create(tx_num: i32, blk: BlockId, offset: i32, val: String) -> Self {
        SetStringRecord {
            tx_num,
            offset,
            val,
            blk,
        }
    }
    
    pub fn write_to_page(&self, page: &mut Page) {
        page.set_int(0, SETSTRING);
        page.set_int(4, self.tx_num);
        page.set_string(8, &self.blk.filename());
        let pos = 8 + Page::max_length(self.blk.filename().len());
        page.set_int(pos, self.blk.number());
        page.set_int(pos + 4, self.offset);
        page.set_string(pos + 8, &self.val);
    }
}