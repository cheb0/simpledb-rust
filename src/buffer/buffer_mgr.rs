use std::io;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use std::cell::UnsafeCell;

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
    buffers: Box<[UnsafeCell<Buffer>]>,
    condvar: Condvar,
}

// This is safe because we ensure exclusive access through the BufferMgrInner
unsafe impl Sync for BufferMgr {}

struct BufferMgrInner {
    pins: Box<[usize]>,
    num_available: usize,
}

impl BufferMgr {
    pub fn new(fm: Arc<FileMgr>, lm: Arc<LogMgr>, numbuffs: usize) -> Self {
        let mut buffers = Vec::with_capacity(numbuffs);
        for _ in 0..numbuffs {
            buffers.push(UnsafeCell::new(Buffer::new(Arc::clone(&fm), Arc::clone(&lm))));
        }
        
        BufferMgr {
            inner: Mutex::new(BufferMgrInner {
                pins: vec![0; numbuffs].into_boxed_slice(),
                num_available: numbuffs,
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
            // Safety: We have exclusive access through the lock
            let buffer = unsafe { &mut *buffer.get() };
            if buffer.modifying_tx() == txnum {
                buffer.flush()?;
            }
        }
        Ok(())
    }
    
    /// Pins the block to a buffer.
    /// If the block is already in a buffer, that buffer is used.
    /// Otherwise, an unpinned buffer is chosen.
    pub fn pin<'a>(&'a self, blk: &BlockId) -> DbResult<&'a mut Buffer> {
        const MAX_TIME: Duration = Duration::from_secs(10); // TODO make configurable
        let start_time = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        let mut pinned_buff_id = self.try_to_pin(&mut inner, blk)?;
        
        while pinned_buff_id.is_none() && start_time.elapsed() < MAX_TIME {
            inner = self.condvar.wait_timeout(inner, MAX_TIME).unwrap().0;
            pinned_buff_id = self.try_to_pin(&mut inner, blk)?;
        }
        
        if pinned_buff_id.is_none() {
            return Err(DbError::BufferAbort("Cannot pin buffer".to_string()));
        }
        
        let idx = pinned_buff_id.unwrap();
        // Safety: We ensure exclusive access to this buffer through the pin count
        let buffer = unsafe { &mut *self.buffers[idx].get() };

        Ok(buffer)
    }
    
    fn try_to_pin(&self, inner: &mut BufferMgrInner, blk: &BlockId) -> DbResult<Option<usize>> {
        // First, check if the block is already in a buffer
        if let Some(idx) = self.find_existing_buffer(blk) {
            if inner.pins[idx] == 0 {
                inner.num_available -= 1;
            }
            inner.pins[idx] += 1;
            
            // Safety: We have exclusive access through the lock
            let buffer = unsafe { &mut *self.buffers[idx].get() };
            buffer.pin();
            
            return Ok(Some(idx));
        }
        
        if let Some(idx) = self.choose_unpinned_buffer(inner) {
            inner.pins[idx] = 1;
            inner.num_available -= 1;
            
            // Safety: We have exclusive access through the lock
            let buffer = unsafe { &mut *self.buffers[idx].get() };
            buffer.assign_to_block(blk.clone())?;
            buffer.pin();
            
            return Ok(Some(idx));
        }
        
        Ok(None)
    }
    
    fn find_existing_buffer(&self, blk: &BlockId) -> Option<usize> {
        for (i, buffer) in self.buffers.iter().enumerate() {
            let buffer = unsafe { &*buffer.get() };
            if let Some(b) = buffer.block() {
                if b == blk {
                    return Some(i);
                }
            }
        }
        None
    }
    
    fn choose_unpinned_buffer(&self, inner: &BufferMgrInner) -> Option<usize> {
        for (i, &pin_count) in inner.pins.iter().enumerate() {
            if pin_count == 0 {
                return Some(i);
            }
        }
        None
    }
    
    /// Unpins the specified buffer.
    pub fn unpin(&self, buff: &mut Buffer) {
        let mut inner = self.inner.lock().unwrap();
        
        // Find the buffer index
        let idx = self.find_buffer_index(buff);
        
        inner.pins[idx] -= 1;
        buff.unpin();

        if !buff.is_pinned() {
            if inner.pins[idx] == 0 {
                inner.num_available += 1;
                self.condvar.notify_all();
            }
        }
    }
    
    /// Finds the index of a buffer in the pool.
    fn find_buffer_index(&self, buff: &Buffer) -> usize {
        let buff_ptr = buff as *const Buffer;
        
        for (i, buffer) in self.buffers.iter().enumerate() {
            let buffer_ptr = buffer.get();
            if buffer_ptr as *const Buffer == buff_ptr {
                return i;
            }
        }
        
        panic!("Buffer not found in pool");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::num;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use tempfile::tempdir;
    use crate::storage::file_mgr::FileMgr;
    use crate::log::LogMgr;
    use crate::storage::page::Page;

    #[test]
    fn test_buffer_manager_very_basic() -> DbResult<()> {
        let temp_dir = tempdir()?;
        let fm = Arc::new(FileMgr::new(temp_dir.path(), 400)?);
        let lm = Arc::new(LogMgr::new(Arc::clone(&fm), "testlog")?);
        let num_blocks = 3;
        for _ in 0..num_blocks {
            fm.append("testfile")?;
        }
        
        let buffer_mgr = BufferMgr::new(Arc::clone(&fm), Arc::clone(&lm), 3);
        
        assert_eq!(buffer_mgr.available(), 3);

        let blk1 = BlockId::new("testfile".to_string(), 1);
        let buffer1 = buffer_mgr.pin(&blk1)?;

        assert_eq!(buffer_mgr.available(), 2);

        Ok(())
    }

    #[test]
    fn test_buffer_manager_basic() -> DbResult<()> {
        let temp_dir = tempdir()?;
        let fm = Arc::new(FileMgr::new(temp_dir.path(), 400)?);
        let lm = Arc::new(LogMgr::new(Arc::clone(&fm), "testlog")?);
        let buffer_mgr = BufferMgr::new(Arc::clone(&fm), Arc::clone(&lm), 3);
        let num_blocks = 5;
        for _ in 0..num_blocks {
            fm.append("testfile")?;
        }
        
        assert_eq!(buffer_mgr.available(), 3);
        
        let blk1 = BlockId::new("testfile".to_string(), 1);
        let blk2 = BlockId::new("testfile".to_string(), 2);
        let blk3 = BlockId::new("testfile".to_string(), 3);
        
        let buffer1 = buffer_mgr.pin(&blk1)?;
        assert_eq!(buffer_mgr.available(), 2);
        
        buffer1.contents_mut().set_int(0, 101);
        buffer1.set_modified(1, 0);
        
        let buffer2 = buffer_mgr.pin(&blk2)?;
        assert_eq!(buffer_mgr.available(), 1);
        
        buffer2.contents_mut().set_int(0, 102);
        buffer2.set_modified(1, 0);
        
        let buffer3 = buffer_mgr.pin(&blk3)?;
        assert_eq!(buffer_mgr.available(), 0);
        
        buffer3.contents_mut().set_int(0, 103);
        buffer3.set_modified(1, 0);
        
        buffer_mgr.unpin(buffer1);
        assert_eq!(buffer_mgr.available(), 1);
        
        let blk4 = BlockId::new("testfile".to_string(), 4);
        let buffer4 = buffer_mgr.pin(&blk4)?;
        assert_eq!(buffer_mgr.available(), 0);
        
        buffer4.contents_mut().set_int(0, 104);
        buffer4.set_modified(1, 0);
        
        // Flush all buffers for transaction 1
        buffer_mgr.flush_all(1)?;
        
        buffer_mgr.unpin(buffer2);
        buffer_mgr.unpin(buffer3);
        buffer_mgr.unpin(buffer4);
        assert_eq!(buffer_mgr.available(), 3);
        
        let buffer = buffer_mgr.pin(&blk1)?;
        assert_eq!(buffer.contents().get_int(0), 101);
        buffer_mgr.unpin(buffer);
        
        let buffer = buffer_mgr.pin(&blk2)?;
        assert_eq!(buffer.contents().get_int(0), 102);
        buffer_mgr.unpin(buffer);
        
        let buffer = buffer_mgr.pin(&blk3)?;
        assert_eq!(buffer.contents().get_int(0), 103);
        buffer_mgr.unpin(buffer);
        
        let buffer = buffer_mgr.pin(&blk4)?;
        assert_eq!(buffer.contents().get_int(0), 104);
        buffer_mgr.unpin(buffer);
        
        Ok(())
    }
    
    #[test]
    fn test_buffer_manager_pinning_same_block() -> DbResult<()> {
        let temp_dir = tempdir()?;
        let fm = Arc::new(FileMgr::new(temp_dir.path(), 400)?);
        let lm = Arc::new(LogMgr::new(Arc::clone(&fm), "testlog")?);
        let num_blocks = 3;
        for _ in 0..num_blocks {
            fm.append("testfile")?;
        }
        
        let buffer_mgr = BufferMgr::new(Arc::clone(&fm), Arc::clone(&lm), 3);
        let blk = BlockId::new("testfile".to_string(), 1);
        
        let buffer1 = buffer_mgr.pin(&blk)?;
        assert_eq!(buffer_mgr.available(), 2);
        
        let buffer2 = buffer_mgr.pin(&blk)?;
        assert_eq!(buffer_mgr.available(), 2);
        
        buffer1.contents_mut().set_int(0, 101);
        buffer1.set_modified(1, 0);
        
        // Both buffers should point to the same data
        assert_eq!(buffer2.contents().get_int(0), 101);
        
        buffer_mgr.unpin(buffer1);
        buffer_mgr.unpin(buffer2);
        assert_eq!(buffer_mgr.available(), 3);
        
        Ok(())
    }
    
    #[test]
    fn test_buffer_manager_waiting_for_buffer() -> DbResult<()> {
        let temp_dir = tempdir()?;
        let fm = Arc::new(FileMgr::new(temp_dir.path(), 400)?);
        let lm = Arc::new(LogMgr::new(Arc::clone(&fm), "testlog")?);

        // Create a buffer manager with a single buffer
        let buffer_mgr = Arc::new(BufferMgr::new(
            Arc::clone(&fm), 
            Arc::clone(&lm), 
            1)
        );
        let num_blocks = 2;
        for _ in 0..num_blocks {
            fm.append("testfile")?;
        }
        
        let blk1 = BlockId::new("testfile".to_string(), 0);
        let blk2 = BlockId::new("testfile".to_string(), 1);
        
        let buffer = buffer_mgr.pin(&blk1)?;
        assert_eq!(buffer_mgr.available(), 0);
        
        // Spawn a thread that tries to pin another block
        let buffer_mgr_clone = Arc::clone(&buffer_mgr);
        let blk2_clone = blk2.clone();
        
        let handle = thread::spawn(move || {
            // This should block until the buffer is available
            let buffer = buffer_mgr_clone.pin(&blk2_clone).unwrap();
            assert_eq!(buffer.contents().get_int(0), 0);
            buffer_mgr_clone.unpin(buffer);
        });
        
        thread::sleep(Duration::from_millis(200));
        
        buffer_mgr.unpin(buffer);
        
        handle.join().unwrap();
        assert_eq!(buffer_mgr.available(), 1);
        
        Ok(())
    }
    
    #[test]
    fn test_buffer_manager_concurrent_access() -> DbResult<()> {
        let temp_dir = tempdir()?;
        let fm = Arc::new(FileMgr::new(temp_dir.path(), 400)?);
        let lm = Arc::new(LogMgr::new(Arc::clone(&fm), "testlog")?);
        let buffer_mgr = Arc::new(
            BufferMgr::new(Arc::clone(&fm), Arc::clone(&lm), 3)
        );
        let num_threads = 5;
        let ops_per_thread = 100;
        let num_blocks = num_threads * ops_per_thread;
        for _ in 0..num_blocks {
            fm.append("testfile")?;
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
                    let buffer = buffer_mgr_clone.pin(&blk).unwrap();
                    
                    let value = (thread_id * 1000 + i) as i32;
                    buffer.contents_mut().set_int(0, value);
                    buffer.set_modified(thread_id as i32, 0);
                    
                    thread::sleep(Duration::from_millis(1));

                    buffer_mgr_clone.unpin(buffer);
                }
            });
            
            handles.push(handle);
        }
        
        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }
        
        // All buffers should be available again
        assert_eq!(buffer_mgr.available(), 3);
        
        Ok(())
    }
    
    #[test]
    fn test_buffer_manager_buffer_abort() -> DbResult<()> {
        let temp_dir = tempdir().unwrap();
        let fm = Arc::new(FileMgr::new(temp_dir.path(), 400).unwrap());
        let lm = Arc::new(LogMgr::new(Arc::clone(&fm), "testlog").unwrap());
        let num_blocks = 3;
        for _ in 0..num_blocks {
            fm.append("testfile")?;
        }
        let buffer_mgr = BufferMgr::new(
            Arc::clone(&fm), 
            Arc::clone(&lm), 
            1
        );
        
        let blk1 = BlockId::new("testfile".to_string(), 1);
        let blk2 = BlockId::new("testfile".to_string(), 2);
        
        let buffer = buffer_mgr.pin(&blk1).unwrap();
        
        // Try to pin another block - this should fail with BufferAbort
        match buffer_mgr.pin(&blk2) {
            Err(DbError::BufferAbort(_)) => {
                // Expected
            },
            Ok(_) => panic!("Expected BufferAbort error"),
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
        
        buffer_mgr.unpin(buffer);
        
        let buffer = buffer_mgr.pin(&blk2).unwrap();
        buffer_mgr.unpin(buffer);
        Ok(())
    }
}
