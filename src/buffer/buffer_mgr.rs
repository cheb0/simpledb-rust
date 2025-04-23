use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use std::cell::RefCell;
use std::collections::HashMap;

use crate::error::{DbError, DbResult};
use crate::storage::block_id::BlockId;
use crate::buffer::buffer::Buffer;
use crate::storage::file_mgr::FileMgr;
use crate::log::LogMgr;

/// Manages the buffer pool, which consists of a collection of Buffer objects.
/// It employs interrior mutability and also is thread-safe
/// TODO Current implementation is naive (and unsafe) and has lots of O(N) functions,
/// need to implement LRU or FIFO and optimize it
pub struct BufferMgr {
    inner: Mutex<BufferMgrInner>,
    buffers: Box<[RefCell<Buffer>]>,
    condvar: Condvar,
}

// This is safe because we ensure exclusive access through the BufferMgrInner
unsafe impl Sync for BufferMgr {}

struct BufferMgrInner {
    pins: Box<[usize]>,
    num_available: usize,
    block_to_buffer_idx: HashMap<BlockId, usize>,
}

pub struct PinnedBufferGuard<'a> {
    buffer_mgr: &'a BufferMgr,
    buffer: &'a RefCell<Buffer>,
    idx: usize,
}

impl BufferMgr {
    pub fn new(file_mgr: Arc<FileMgr>, log_mgr: Arc<LogMgr>, buffer_count: usize) -> Self {
        let mut buffers = Vec::with_capacity(buffer_count);
        for _ in 0..buffer_count {
            buffers.push(RefCell::new(
                Buffer::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr))
            ));
        }
        
        BufferMgr {
            inner: Mutex::new(BufferMgrInner {
                pins: vec![0; buffer_count].into_boxed_slice(),
                num_available: buffer_count,
                block_to_buffer_idx: HashMap::new(),
            }),
            buffers: buffers.into_boxed_slice(),
            condvar: Condvar::new(),
        }
    }

    pub fn available(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.num_available
    }

    pub fn flush_all(&self, txnum: i32) -> DbResult<()> {
        let _guard = self.inner.lock().unwrap();
        
        for buffer in self.buffers.iter() {
            let mut buffer = buffer.borrow_mut();
            if buffer.modifying_tx() == txnum {
                buffer.flush()?;
            }
        }
        Ok(())
    }
    
    /// Pins the block to a buffer.
    /// If the block is already in a buffer, that buffer is used.
    /// Otherwise, an unpinned buffer is chosen.
    pub fn pin<'a>(&'a self, blk: &BlockId) -> DbResult<PinnedBufferGuard<'a>> {
        const MAX_TIME: Duration = Duration::from_secs(10);
        let start_time = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        let mut pinned_buff_id = self.try_to_pin(&mut inner, blk)?;
        
        while pinned_buff_id.is_none() && start_time.elapsed() < MAX_TIME {
            inner = self.condvar.wait_timeout(inner, MAX_TIME).unwrap().0;
            pinned_buff_id = self.try_to_pin(&mut inner, blk)?;
        }
        
        if let Some(idx) = pinned_buff_id {
            Ok(PinnedBufferGuard {
                buffer_mgr: self,
                buffer: &self.buffers[idx],
                idx,
            })
        } else {
            Err(DbError::BufferAbort("Cannot pin buffer".to_string()))
        }
    }
    
    fn try_to_pin(&self, inner: &mut BufferMgrInner, blk: &BlockId) -> DbResult<Option<usize>> {
        if let Some(&idx) = inner.block_to_buffer_idx.get(blk) {
            if inner.pins[idx] == 0 {
                inner.num_available -= 1;
            }
            inner.pins[idx] += 1;
            
            let mut buffer = self.buffers[idx].borrow_mut();
            buffer.pin();
            
            return Ok(Some(idx));
        }
        
        if let Some(idx) = self.find_unpinned_buffer(inner) {
            inner.block_to_buffer_idx.insert(blk.clone(), idx);
            inner.pins[idx] = 1;
            inner.num_available -= 1;
            
            let mut buffer = self.buffers[idx].borrow_mut();
            buffer.assign_to_block(blk.clone())?;
            buffer.pin();
            
            return Ok(Some(idx));
        }
        
        Ok(None)
    }
    
    fn find_unpinned_buffer(&self, inner: &BufferMgrInner) -> Option<usize> {
        // TODO O(N)
        for (i, &pin_count) in inner.pins.iter().enumerate() {
            if pin_count == 0 {
                return Some(i);
            }
        }
        None
    }
    
    fn unpin_internal(&self, idx: usize) {
        let mut inner = self.inner.lock().unwrap();
        let mut buffer = self.buffers[idx].borrow_mut();
        
        inner.pins[idx] -= 1;
        buffer.unpin();

        if !buffer.is_pinned() && inner.pins[idx] == 0 {
            if let Some(block) = buffer.block() {
                inner.block_to_buffer_idx.remove(&block);
            }
            inner.num_available += 1;
            self.condvar.notify_all();
        }
    }
}

impl<'a> PinnedBufferGuard<'a> {
    pub fn borrow_mut(&self) -> std::cell::RefMut<'_, Buffer> {
        self.buffer.borrow_mut()
    }

    pub fn borrow(&self) -> std::cell::Ref<'_, Buffer> {
        self.buffer.borrow()
    }
}

impl<'a> Drop for PinnedBufferGuard<'a> {
    fn drop(&mut self) {
        self.buffer_mgr.unpin_internal(self.idx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::sync::{Arc, Barrier};
    use crate::storage::file_mgr::FileMgr;
    use crate::log::LogMgr;
    use tempfile::TempDir;

    #[test]
    fn test_buffer_pin_and_modify() -> DbResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_mgr = Arc::new(FileMgr::new(temp_dir.path().to_path_buf(), 400)?);
        let log_mgr = Arc::new(LogMgr::new(Arc::clone(&file_mgr), "testlog")?);
        let buffer_mgr = BufferMgr::new(file_mgr.clone(), log_mgr.clone(), 3);
        let num_blocks = 3;
        for _ in 0..num_blocks {
            file_mgr.append("testfile")?;
        }

        let block = BlockId::new("testfile".to_string(), 1);
        let pinned_buf = buffer_mgr.pin(&block)?;

        assert_eq!(buffer_mgr.available(), 2);

        {
            let mut buffer: std::cell::RefMut<'_, Buffer> = pinned_buf.borrow_mut();
            buffer.contents_mut().set_int(0, 123);
            buffer.set_modified(1, 0); // Set as modified by transaction 1
        }

        assert_eq!(buffer_mgr.available(), 2);

        drop(pinned_buf);

        assert_eq!(buffer_mgr.available(), 3);

        let pinned_guard = buffer_mgr.pin(&block)?;
        {
            let buffer = pinned_guard.borrow();
            assert_eq!(buffer.contents().get_int(0), 123);
        }

        Ok(())
    }

    #[test]
    fn test_buffer_manager_waiting_for_buffer() -> DbResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_mgr = Arc::new(FileMgr::new(temp_dir.path(), 400)?);
        let log_mgr = Arc::new(LogMgr::new(Arc::clone(&file_mgr), "testlog")?);
        let buffer_mgr = Arc::new(BufferMgr::new(
            Arc::clone(&file_mgr), 
            Arc::clone(&log_mgr), 
            1)
        );

        let num_blocks = 10;
        for _ in 0..num_blocks {
            file_mgr.append("testfile")?;
        }
        
        let blk1 = BlockId::new("testfile".to_string(), 0);
        let blk2 = BlockId::new("testfile".to_string(), 1);
        
        let guard1 = buffer_mgr.pin(&blk1)?;
        assert_eq!(buffer_mgr.available(), 0);
        
        let buffer_mgr_clone = Arc::clone(&buffer_mgr);
        let blk2_clone = blk2.clone();
        
        let handle = thread::spawn(move || {
            let guard = buffer_mgr_clone.pin(&blk2_clone).unwrap();
            {
                let buffer = guard.borrow();
                assert_eq!(buffer.contents().get_int(0), 0);
            }
        });
        
        thread::sleep(Duration::from_millis(200));
        
        drop(guard1);
        
        handle.join().unwrap();
        assert_eq!(buffer_mgr.available(), 1);
        
        Ok(())
    }

    #[test]
    fn test_buffer_manager_concurrent_access() -> DbResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_mgr = Arc::new(FileMgr::new(temp_dir.path(), 400)?);
        let log_mgr = Arc::new(LogMgr::new(Arc::clone(&file_mgr), "testlog")?);
        let buffer_mgr = Arc::new(
            BufferMgr::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), 3)
        );

        let num_threads = 5;
        let ops_per_thread = 100;
        let num_blocks = num_threads * ops_per_thread;
        for _ in 0..num_blocks {
            file_mgr.append("testfile")?;
        }

        let barrier = Arc::new(Barrier::new(num_threads));
        
        let mut handles = Vec::new();
        for thread_id in 0..num_threads {
            let buffer_mgr_clone = Arc::clone(&buffer_mgr);
            let barrier_clone = Arc::clone(&barrier);
            
            let handle = thread::spawn(move || {
                barrier_clone.wait();
                
                for i in 0..ops_per_thread {
                    let blk = BlockId::new("testfile".to_string(), (thread_id * 100 + i) as i32);
                    let guard = buffer_mgr_clone.pin(&blk).unwrap();
                    
                    {
                        let mut buffer = guard.borrow_mut();
                        let value = (thread_id * 1000 + i) as i32;
                        buffer.contents_mut().set_int(0, value);
                        buffer.set_modified(thread_id as i32, 0);
                    }
                    
                    thread::sleep(Duration::from_millis(1));
                }
            });
            
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        assert_eq!(buffer_mgr.available(), 3);
        
        Ok(())
    }

    #[test]
    fn test_buffer_manager_buffer_abort() -> DbResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_mgr = Arc::new(FileMgr::new(temp_dir.path(), 400)?);
        let log_mgr = Arc::new(LogMgr::new(Arc::clone(&file_mgr), "testlog")?);
        let num_blocks = 3;
        for _ in 0..num_blocks {
            file_mgr.append("testfile")?;
        }
        let buffer_mgr = BufferMgr::new(
            Arc::clone(&file_mgr), 
            Arc::clone(&log_mgr), 
            1
        );
        
        let blk1 = BlockId::new("testfile".to_string(), 1);
        let blk2 = BlockId::new("testfile".to_string(), 2);
        
        let guard1 = buffer_mgr.pin(&blk1)?;

        {
            let mut buffer = guard1.borrow_mut();
            buffer.contents_mut().set_int(0, 5);
        }
        
        match buffer_mgr.pin(&blk2) {
            Err(DbError::BufferAbort(_)) => {
                // Expected
            },
            Ok(_) => panic!("Expected BufferAbort error"),
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
        
        drop(guard1);
        
        let guard2 = buffer_mgr.pin(&blk2)?;
        {
            let mut buffer = guard2.borrow_mut();
            buffer.contents_mut().set_int(0, 5);
        }
        Ok(())
    }

    #[test]
    fn test_pin_same_block_returns_same_buffer() -> DbResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_mgr = Arc::new(FileMgr::new(temp_dir.path(), 400)?);
        let log_mgr = Arc::new(LogMgr::new(Arc::clone(&file_mgr), "testlog")?);
        let buffer_mgr = BufferMgr::new(file_mgr.clone(), log_mgr.clone(), 3);
        file_mgr.append("testfile")?;
        
        let blk = BlockId::new("testfile".to_string(), 0);
        
        let first_guard = buffer_mgr.pin(&blk)?;
        let first_buffer_ptr = first_guard.buffer as *const _;
        
        let second_guard = buffer_mgr.pin(&blk)?;
        let second_buffer_ptr = second_guard.buffer as *const _;
        
        assert_eq!(first_buffer_ptr, second_buffer_ptr);
        
        {
            let inner = buffer_mgr.inner.lock().unwrap();
            let idx = inner.block_to_buffer_idx.get(&blk).unwrap();
            assert_eq!(inner.pins[*idx], 2);
        }
        
        drop(first_guard);
        drop(second_guard);
        
        assert_eq!(buffer_mgr.available(), 3);
        Ok(())
    }
}