/// A set integer log record.
pub struct SetIntRecord {
    tx_num: i32,
    offset: i32,
    val: i32,
    blk: BlockId,
}

impl SetIntRecord {
    pub fn new(page: &Page) -> Self {
        let tx_num = page.get_int(4);
        let filename = page.get_string(8);
        let block_num = page.get_int(8 + Page::max_length(filename.len()));
        let offset = page.get_int(12 + Page::max_length(filename.len()));
        let val = page.get_int(16 + Page::max_length(filename.len()));
        
        SetIntRecord {
            tx_num,
            offset,
            val,
            blk: BlockId::new(filename, block_num),
        }
    }

    pub fn create(tx_num: i32, blk: BlockId, offset: i32, val: i32) -> Self {
        SetIntRecord {
            tx_num,
            offset,
            val,
            blk,
        }
    }
    
    pub fn write_to_page(&self, page: &mut Page) {
        page.set_int(0, SETINT);
        page.set_int(4, self.tx_num);
        page.set_string(8, &self.blk.filename());
        let pos = 8 + Page::max_length(self.blk.filename().len());
        page.set_int(pos, self.blk.number());
        page.set_int(pos + 4, self.offset);
        page.set_int(pos + 8, self.val);
    }
}

impl LogRecord for SetIntRecord {
    fn op(&self) -> i32 {
        SETINT
    }

    fn tx_number(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, tx_num: i32, buffer_mgr: &Arc<BufferMgr>) -> DbResult<()> {
        Ok(())
    }
}