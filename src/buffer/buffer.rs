use std::io;
use std::sync::Arc;

use crate::storage::BlockId;
use crate::storage::FileMgr;
use crate::log::LogMgr;
use crate::storage::Page;

/// Represents a buffer, which is a memory region that contains a disk block.
pub struct Buffer {
    file_mgr: Arc<dyn FileMgr>,
    log_mgr: Arc<LogMgr>,
    page: Page,
    block_id: Option<BlockId>,
    tx_id: i32,
    lsn: i32,
}

impl Buffer {
    pub fn new(file_mgr: Arc<dyn FileMgr>, log_mgr: Arc<LogMgr>) -> Self {
        let block_size = file_mgr.block_size();
        Buffer {
            file_mgr,
            log_mgr,
            page: Page::new(block_size),
            block_id: None,
            tx_id: -1,
            lsn: -1,
        }
    }

    pub fn page(&self) -> &Page {
        &self.page
    }

    pub fn contents_mut(&mut self) -> &mut Page {
        &mut self.page
    }

    pub fn block(&self) -> Option<&BlockId> {
        self.block_id.as_ref()
    }

    pub fn set_modified(&mut self, txnum: i32, lsn: i32) {
        self.tx_id = txnum;
        if lsn >= 0 {
            self.lsn = lsn;
        }
    }

    pub fn modifying_tx(&self) -> i32 {
        self.tx_id
    }

    /// Assigns this buffer to the specified block.
    /// If the buffer was previously assigned to a block,
    /// that block is written to disk.
    pub fn assign_to_block(&mut self, blk: BlockId) -> io::Result<()> {
        self.flush()?;
        self.block_id = Some(blk.clone());
        self.file_mgr.read(&blk, &mut self.page)?;
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        if self.tx_id >= 0 {
            self.log_mgr.flush(self.lsn)?;
            if let Some(blk) = &self.block_id {
                self.file_mgr.write(blk, &self.page)?;
            }
            self.tx_id = -1;
        }
        Ok(())
    }
}