use crate::error::DbResult;
use crate::storage::BlockId;
use std::collections::HashMap;

use super::{BufferMgr, PinnedBufferGuard};

// TODO currently this class duplicates BufferMgr functionality
pub struct BufferList<'a> {
    buffers: HashMap<BlockId, PinnedBufferGuard<'a>>,
    pins: HashMap<BlockId, usize>,
    buffer_mgr: &'a BufferMgr,
}

impl<'a> BufferList<'a> {
    pub fn new(buffer_mgr: &'a BufferMgr) -> Self {
        BufferList {
            buffers: HashMap::new(),
            pins: HashMap::new(),
            buffer_mgr,
        }
    }

    // That's not so convinient but we keep it close to original implementation
    pub fn get_buffer(&self, blk: &BlockId) -> Option<&PinnedBufferGuard<'a>> {
        self.buffers.get(blk)
    }

    pub fn pin(&mut self, blk: &BlockId) -> DbResult<()> {
        if let Some(count) = self.pins.get_mut(&blk) {
            *count += 1;
            return Ok(());
        }
        let guard = self.buffer_mgr.pin(&blk)?;
        self.buffers.insert(blk.clone(), guard);
        self.pins.insert(blk.clone(), 1);
        Ok(())
    }

    pub fn unpin(&mut self, blk: &BlockId) {
        if let Some(count) = self.pins.get_mut(blk) {
            *count -= 1;
            if *count == 0 {
                if let Some(guard) = self.buffers.remove(blk) {
                    drop(guard);
                }
                self.pins.remove(blk);
            }
        }
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
    use crate::log::LogMgr;
    use crate::storage::{FileStorageMgr, StorageMgr};
    use std::sync::Arc;
    use tempfile::TempDir;

    struct TestEnvironment {
        _temp_dir: TempDir, // Keep temp_dir alive
        storage_mgr: Arc<dyn StorageMgr>,
        buffer_mgr: BufferMgr,
    }

    impl TestEnvironment {
        fn new() -> DbResult<Self> {
            let temp_dir = TempDir::new().unwrap();
            let storage_mgr: Arc<dyn StorageMgr> =
                Arc::new(FileStorageMgr::new(temp_dir.path(), 400)?);
            let log_mgr = Arc::new(LogMgr::new(Arc::clone(&storage_mgr), "testlog")?);
            let buffer_mgr = BufferMgr::new(Arc::clone(&storage_mgr), Arc::clone(&log_mgr), 3);

            Ok(TestEnvironment {
                _temp_dir: temp_dir,
                storage_mgr,
                buffer_mgr,
            })
        }

        fn create_buffer_list(&self) -> BufferList<'_> {
            BufferList::new(&self.buffer_mgr)
        }
    }

    #[test]
    fn test_buffer_list() -> DbResult<()> {
        let env = TestEnvironment::new()?;
        let mut buffer_list = env.create_buffer_list();

        let blocks_cnt = 3;
        for _ in 0..blocks_cnt {
            env.storage_mgr.append("testfile")?;
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

    #[test]
    fn test_pin_already_pinned_block() -> DbResult<()> {
        let env = TestEnvironment::new()?;
        let mut buffer_list = env.create_buffer_list();

        env.storage_mgr.append("testfile")?;

        let block = BlockId::new("testfile".to_string(), 0);
        buffer_list.pin(&block)?;

        let first_buffer_ptr = buffer_list.get_buffer(&block).unwrap() as *const _;

        buffer_list.pin(&block)?;

        let second_buffer_ptr = buffer_list.get_buffer(&block).unwrap() as *const _;

        assert_eq!(first_buffer_ptr, second_buffer_ptr);
        Ok(())
    }
}
