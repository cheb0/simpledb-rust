use std::collections::{HashMap, HashSet};
use crate::storage::block_id::BlockId;
use crate::error::DbResult;

use super::buffer_mgr::{BufferMgr, PinnedBufferGuard};

pub struct BufferList<'a> {
    buffers: HashMap<BlockId, PinnedBufferGuard<'a>>,
    pins: HashSet<BlockId>,
    buffer_mgr: &'a BufferMgr,
}

impl<'a> BufferList<'a> {
    pub fn new(buffer_mgr: &'a BufferMgr) -> Self {
        BufferList {
            buffers: HashMap::new(),
            pins: HashSet::new(),
            buffer_mgr,
        }
    }
    
    // That's not so convinient but we keep it close to original implementation
    pub fn get_buffer(&self, blk: &BlockId) -> Option<&PinnedBufferGuard<'a>> {
        self.buffers.get(blk)
    }

    pub fn pin(&mut self, blk: &BlockId) -> DbResult<()> {
        // if there is a double pin, then the previous guard is dropped. that's ok for now
        let guard = self.buffer_mgr.pin(&blk)?;
        self.buffers.insert(blk.clone(), guard);
        self.pins.insert(blk.clone());
        Ok(())
    }
    
    pub fn unpin(&mut self, blk: &BlockId) {
        if let Some(guard) = self.buffers.remove(blk) {
            drop(guard);
        }
        self.pins.remove(blk);
    }
    
    pub fn unpin_all(&mut self) {
        self.buffers.clear();
        self.pins.clear();
    }
}

impl<'a> Drop for BufferList<'a> {
    fn drop(&mut self) {
        self.unpin_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::storage::file_mgr::FileMgr;
    use crate::log::LogMgr;
    use tempfile::TempDir;

    #[test]
    fn test_buffer_list() -> DbResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_mgr = Arc::new(FileMgr::new(temp_dir.path(), 400)?);
        let log_mgr = Arc::new(LogMgr::new(Arc::clone(&file_mgr), "testlog")?);
        let buffer_mgr = BufferMgr::new(file_mgr.clone(), log_mgr.clone(), 3);
        let mut buffer_list = BufferList::new(&buffer_mgr);
        let num_blocks = 3;
        for _ in 0..num_blocks {
            file_mgr.append("testfile")?;
        }

        let block1 = BlockId::new("testfile".to_string(), 1);
        buffer_list.pin(&block1)?;

        assert!(buffer_list.get_buffer(&block1).is_some());

        let block2 = BlockId::new("testfile".to_string(), 2);
        buffer_list.pin(&block2)?;
        assert!(buffer_list.get_buffer(&block2).is_some());

        buffer_list.unpin(&block1);
        assert!(buffer_list.get_buffer(&block1).is_none());
        assert!(buffer_list.get_buffer(&block2).is_some());

        buffer_list.unpin_all();
        assert!(buffer_list.get_buffer(&block2).is_none());

        Ok(())
    }
}