use std::io;
use std::sync::{Arc, Mutex};

use crate::storage::BlockId;
use crate::storage::file_mgr::FileMgr;
use crate::storage::page::Page;

/// Manages the database log, which is used for recovery.
// It employs interrior mutability and also is thread-safe
pub struct LogMgr {
    file_mgr: Arc<FileMgr>,
    log_file: String,
    inner: Mutex<LogMgrInner>,
}

struct LogMgrInner {
    log_page: Page,
    current_blk: BlockId,
    latest_lsn: i32,
    last_saved_lsn: i32,
}

impl LogMgr {
    pub fn new(file_mgr: Arc<FileMgr>, log_file: &str) -> io::Result<Self> {
        let block_size = file_mgr.block_size();
        let mut log_page = Page::new(block_size);
        log_page.set_int(0, file_mgr.block_size() as i32);
        
        let block_count = file_mgr.block_count(log_file)?;
        let current_blk = if block_count == 0 {
            Self::append_new_block(&file_mgr, log_file)?
        } else {
            let blk = BlockId::new(log_file.to_string(), block_count - 1);
            file_mgr.read(&blk, &mut log_page)?;
            blk
        };
        
        Ok(LogMgr {
            file_mgr,
            log_file: log_file.to_string(),
            inner: Mutex::new(LogMgrInner {
                log_page,
                current_blk,
                latest_lsn: 0,
                last_saved_lsn: 0,
            }),
        })
    }
    
    // Helper method to append a new block to the log file
    fn append_new_block(fm: &FileMgr, log_file: &str) -> io::Result<BlockId> {
        let blk = fm.append(log_file)?;
        let blocksize = fm.block_size();
        let mut logpage = Page::new(blocksize);
        logpage.set_int(0, blocksize as i32);
        fm.write(&blk, &logpage)?;
        Ok(blk)
    }
    
    /// Writes the current log page to disk.
    fn flush_internal(&self, inner: &mut LogMgrInner) -> io::Result<()> {
        self.file_mgr.write(&inner.current_blk, &inner.log_page)?;
        inner.last_saved_lsn = inner.latest_lsn;
        Ok(())
    }

    pub fn flush(&self, lsn: i32) -> io::Result<()> {
        let mut inner: std::sync::MutexGuard<'_, LogMgrInner> = self.inner.lock().unwrap();
        if lsn >= inner.last_saved_lsn {
            return self.flush_internal(&mut inner);
        }
        Ok(())
    }
    
    /// Appends a log record to the log.
    /// Returns the LSN (Log Sequence Number) of the appended record.
    /// This method is thread-safe.
    pub fn append(&self, record: &[u8]) -> io::Result<i32> {
        let mut inner = self.inner.lock().unwrap();
        
        let boundary = inner.log_page.get_int(0);
        
        let rec_size: usize = record.len();
        let bytes_needed = rec_size + std::mem::size_of::<i32>();
        
        // Check if there's enough space in the current block
        if (boundary - bytes_needed as i32) < std::mem::size_of::<i32>() as i32 {
            self.flush_internal(&mut inner)?;
            
            inner.current_blk = Self::append_new_block(&self.file_mgr, &self.log_file)?;
            inner.log_page = Page::new(self.file_mgr.block_size());
            inner.log_page.set_int(0, self.file_mgr.block_size() as i32);
            let boundary = inner.log_page.get_int(0);
            let recpos = boundary - bytes_needed as i32;
            
            // Write the record and update the boundary
            inner.log_page.set_bytes(recpos as usize, record);
            inner.log_page.set_int(0, recpos);
        } else {
            // Calculate position for the new record
            let recpos = boundary - bytes_needed as i32;
            
            // Write the record and update the boundary
            inner.log_page.set_bytes(recpos as usize, record);
            inner.log_page.set_int(0, recpos);
        }
        
        inner.latest_lsn += 1;
        
        Ok(inner.latest_lsn)
    }
    
    /// Returns an iterator over all log records, starting with the most recent.
    pub fn iterator(&self) -> io::Result<LogIterator> {
        let mut inner = self.inner.lock().unwrap();
        self.flush_internal(&mut inner)?;
        LogIterator::new(&self.file_mgr, inner.current_blk.clone())
    }
}

/// An iterator over log records, starting from the most recent and moving backwards.
pub struct LogIterator<'a> {
    file_mgr: &'a Arc<FileMgr>,
    blk: BlockId,
    page: Page,
    current_pos: usize,
    boundary: usize,
}

impl<'a> LogIterator<'a> {
    fn new(file_mgr: &'a Arc<FileMgr>, blk: BlockId) -> io::Result<Self> {
        let page = Page::new(file_mgr.block_size());
        let mut iter = LogIterator {
            file_mgr,
            blk: blk.clone(),
            page,
            current_pos: 0,
            boundary: 0,
        };
        iter.move_to_block(&blk)?;
        Ok(iter)
    }
    
    fn move_to_block(&mut self, blk: &BlockId) -> io::Result<()> {
        self.file_mgr.read(blk, &mut self.page)?;
        self.boundary = self.page.get_int(0) as usize;
        self.current_pos = self.boundary;
        Ok(())
    }
    
    pub fn has_next(&self) -> bool {
        self.current_pos < self.file_mgr.block_size() || self.blk.number() > 0
    }
    
    pub fn next(&mut self) -> io::Result<Vec<u8>> {
        if self.current_pos == self.file_mgr.block_size() {
            let new_blk = BlockId::new(self.blk.file_name().to_string(), self.blk.number() - 1);
            self.blk = new_blk.clone();
            self.move_to_block(&new_blk)?;
        }
        
        let record_bytes = self.page.get_bytes(self.current_pos);
        self.current_pos += std::mem::size_of::<i32>() + record_bytes.len();
        
        Ok(record_bytes)
    }
}

impl Drop for LogMgr {
    fn drop(&mut self) {
        // Try to acquire the lock and flush any pending changes
        if let Ok(mut inner) = self.inner.lock() {
            if inner.latest_lsn > inner.last_saved_lsn {
                let _ = self.flush_internal(&mut inner);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::DbResult;

    use super::*;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn test_log_manager_basic() -> DbResult<()> {
        let temp_dir = tempdir()?;
        let file_mgr: Arc<FileMgr> = Arc::new(FileMgr::new(temp_dir.path(), 400)?);
        let log_mgr = LogMgr::new(Arc::clone(&file_mgr), "testlog")?;
        
        let test_record = b"This is a test log record";
        let lsn = log_mgr.append(test_record)?;
        assert_eq!(lsn, 1); // First LSN should be 1
        
        // Retrieve the record using an iterator
        let mut iter = log_mgr.iterator()?;
        assert!(iter.has_next());
        let retrieved_rec: Vec<u8> = iter.next()?;
        assert_eq!(retrieved_rec, test_record);
        assert!(!iter.has_next());
        
        Ok(())
    }
    
    #[test]
    fn test_log_manager_multiple_records() -> DbResult<()> {
        let temp_dir = tempdir()?;
        let file_mgr = Arc::new(FileMgr::new(temp_dir.path(), 400)?);
        let log_mgr = LogMgr::new(Arc::clone(&file_mgr), "testlog")?;
        
        let records = vec![
            b"First log record".to_vec(),
            b"Second log record".to_vec(),
            b"Third log record".to_vec(),
            b"Fourth log record".to_vec(),
            b"Fifth log record".to_vec(),
        ];
        
        let mut lsns = Vec::new();
        for rec in &records {
            let lsn = log_mgr.append(rec)?;
            lsns.push(lsn);
        }
        
        for (i, lsn) in lsns.iter().enumerate() {
            assert_eq!(*lsn, (i + 1) as i32);
        }
        
        let mut iter = log_mgr.iterator()?;
        let mut retrieved_records = Vec::new();
        
        while iter.has_next() {
            retrieved_records.push(iter.next()?);
        }
        
        // Records should be in reverse order (newest first)
        retrieved_records.reverse();
        assert_eq!(retrieved_records, records);
        
        Ok(())
    }
    
    #[test]
    fn test_log_manager_block_boundary() -> DbResult<()> {
        let temp_dir = tempdir()?;
        let block_size = 100;
        let file_mgr = Arc::new(FileMgr::new(temp_dir.path(), block_size)?);
        let log_mgr = LogMgr::new(Arc::clone(&file_mgr), "testlog")?;
        
        let mut records = Vec::new();
        for i in 0..1000 {
            let rec = format!("Record #{}", i).into_bytes();
            records.push(rec);
        }
        
        for rec in &records {
            log_mgr.append(rec)?;
        }
        
        let mut iter = log_mgr.iterator()?;
        let mut retrieved_records = Vec::new();
        
        while iter.has_next() {
            retrieved_records.push(iter.next()?);
        }
        
        // Records should be in reverse order (newest first)
        retrieved_records.reverse();
        assert_eq!(retrieved_records, records);
        
        Ok(())
    }
    
    #[test]
    fn test_log_manager_persistence() -> DbResult<()> {
        let temp_dir = tempdir()?;
        let fm = Arc::new(FileMgr::new(temp_dir.path(), 400)?);
        let records = vec![
            b"First log record".to_vec(),
            b"Second log record".to_vec(),
            b"Third log record".to_vec(),
        ];
        
        // First session: create log manager and append records
        {
            let log_mgr = LogMgr::new(Arc::clone(&fm), "testlog")?;
            
            for rec in &records {
                log_mgr.append(rec)?;
            }            
            // LogMgr will be dropped here, which should flush any pending changes
        }
        
        // Second session: create a new log manager and read the records
        {
            let log_mgr = LogMgr::new(Arc::clone(&fm), "testlog")?;
            
            // Retrieve records using an iterator
            let mut iter: LogIterator<'_> = log_mgr.iterator()?;
            let mut retrieved_records = Vec::new();
            
            while iter.has_next() {
                retrieved_records.push(iter.next()?);
            }
            retrieved_records.reverse();
            assert_eq!(retrieved_records, records);
        }
        
        Ok(())
    }

    #[test]
    fn test_log_manager_thread_safety() -> DbResult<()> {
        use std::thread;
        use std::sync::{Arc, Barrier};

        let temp_dir = tempdir()?;
        let fm = Arc::new(FileMgr::new(temp_dir.path(), 4096)?);
        let log_mgr = Arc::new(LogMgr::new(Arc::clone(&fm), "testlog")?);

        let thread_count = 10;
        let records_per_thread = 50000;
        let barrier = Arc::new(Barrier::new(thread_count));

        let mut handles = Vec::new();

        for thread_id in 0..thread_count {
            let log_mgr_clone = Arc::clone(&log_mgr);
            let barrier_clone = Arc::clone(&barrier);

            let handle = thread::spawn(move || {
                barrier_clone.wait();
                let mut lsns = Vec::new();

                for i in 0..records_per_thread {
                    let record = format!("Thread {} - Record {}", thread_id, i).into_bytes();
                    match log_mgr_clone.append(&record) {
                        Ok(lsn) => lsns.push((record, lsn)),
                        Err(e) => panic!("Error appending record: {}", e),
                    }
                }
                
                lsns
            });

            handles.push(handle);
        }

        let mut all_records = Vec::new();
        for handle in handles {
            let thread_records = handle.join().unwrap();
            all_records.extend(thread_records);
        }

        all_records.sort_by_key(|(_, lsn)| *lsn);

        for (i, (_, lsn)) in all_records.iter().enumerate() {
            assert_eq!(*lsn, (i + 1) as i32);
        }
        
        let mut iter = log_mgr.iterator()?;
        let mut retrieved_records = Vec::new();

        while iter.has_next() {
            retrieved_records.push(iter.next()?);
        }

        retrieved_records.reverse();

        assert_eq!(retrieved_records.len(), thread_count * records_per_thread);
        for (i, (record, _)) in all_records.iter().enumerate() {
            assert_eq!(&retrieved_records[i], record);
        }

        Ok(())
    }
} 