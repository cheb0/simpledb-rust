use std::io;
use std::sync::Arc;

use crate::storage::block_id::BlockId;
use crate::storage::file_mgr::FileMgr;
use crate::log::LogMgr;
use crate::storage::page::Page;

/// Represents a buffer, which is a memory region that contains a disk block.
pub struct Buffer {
    fm: Arc<FileMgr>,
    lm: Arc<LogMgr>,
    contents: Page,
    blk: Option<BlockId>,
    pins: i32,
    txnum: i32,
    lsn: i32,
}

impl Buffer {
    pub fn new(fm: Arc<FileMgr>, lm: Arc<LogMgr>) -> Self {
        let blocksize = fm.block_size();
        Buffer {
            fm,
            lm,
            contents: Page::new(blocksize),
            blk: None,
            pins: 0,
            txnum: -1,
            lsn: -1,
        }
    }

    pub fn contents(&self) -> &Page {
        &self.contents
    }

    pub fn contents_mut(&mut self) -> &mut Page {
        &mut self.contents
    }

    pub fn block(&self) -> Option<&BlockId> {
        self.blk.as_ref()
    }

    pub fn set_modified(&mut self, txnum: i32, lsn: i32) {
        self.txnum = txnum;
        if lsn >= 0 {
            self.lsn = lsn;
        }
    }

    pub fn is_pinned(&self) -> bool {
        self.pins > 0
    }

    pub fn modifying_tx(&self) -> i32 {
        self.txnum
    }

    /// Assigns this buffer to the specified block.
    /// If the buffer was previously assigned to a block,
    /// that block is written to disk.
    pub fn assign_to_block(&mut self, blk: BlockId) -> io::Result<()> {
        self.flush()?;
        self.blk = Some(blk.clone());
        self.fm.read(&blk, &mut self.contents)?;
        self.pins = 0;
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        if self.txnum >= 0 {
            self.lm.flush(self.lsn)?;
            if let Some(blk) = &self.blk {
                self.fm.write(blk, &self.contents)?;
            }
            self.txnum = -1;
        }
        Ok(())
    }

    pub fn pin(&mut self) {
        self.pins += 1;
    }

    pub fn unpin(&mut self) {
        self.pins -= 1;
    }
}