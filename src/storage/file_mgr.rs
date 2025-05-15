use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::error::DbResult;

use super::{BlockId, Page};

/// Manages the database files, handling reading and writing of pages to disk.
pub struct FileMgr {
    db_directory: PathBuf,
    block_size: usize,
    is_new: bool,
    open_files: Mutex<HashMap<String, File>>,
}

impl FileMgr {
    pub fn new<P: AsRef<Path>>(db_directory: P, block_size: usize) -> DbResult<Self> {
        let db_path = db_directory.as_ref().to_path_buf();
        let is_new = !db_path.exists();

        if is_new {
            fs::create_dir_all(&db_path)?;
        }

        if db_path.exists() {
            for entry in fs::read_dir(&db_path)? {
                let entry = entry?;
                let filename = entry.file_name();
                let filename_str = filename.to_string_lossy();
                if filename_str.starts_with("temp") {
                    fs::remove_file(entry.path())?;
                }
            }
        }

        Ok(FileMgr {
            db_directory: db_path,
            block_size,
            is_new,
            open_files: Mutex::new(HashMap::new()),
        })
    }

    /// Reads a block from disk into the provided page.
    pub fn read(&self, blk: &BlockId, page: &mut Page) -> io::Result<()> {
        let mut file = self.get_file(&blk.file_name())?;
        let pos = blk.number() as u64 * self.block_size as u64;
        file.seek(SeekFrom::Start(pos))?;
        
        let buffer = page.contents_mut();
        file.read_exact(buffer)?;
        
        Ok(())
    }

    /// Writes a page to the specified block on disk.
    pub fn write(&self, blk: &BlockId, page: &Page) -> io::Result<()> {
        let mut file = self.get_file(&blk.file_name())?;
        let pos = blk.number() as u64 * self.block_size as u64;
        file.seek(SeekFrom::Start(pos))?;
        
        // Write the page's buffer to disk
        file.write_all(page.contents())?;
        file.flush()?;
        
        Ok(())
    }

    /// Appends a new block to the end of the specified file and returns its BlockId.
    pub fn append(&self, filename: &str) -> io::Result<BlockId> {
        let new_block_num = self.block_count(filename)?;
        let blk = BlockId::new(filename.to_string(), new_block_num);
        
        let mut file = self.get_file(&blk.file_name())?;
        let pos = blk.number() as u64 * self.block_size as u64;
        file.seek(SeekFrom::Start(pos))?;
        
        let zeros = vec![0; self.block_size];
        file.write_all(&zeros)?;
        file.flush()?;
        
        Ok(blk)
    }

    /// Returns the number of blocks in the specified file.
    pub fn block_count(&self, filename: &str) -> io::Result<i32> {
        let file = self.get_file(filename)?;
        let file_size = file.metadata()?.len();
        Ok((file_size / self.block_size as u64) as i32)
    }

    pub fn is_new(&self) -> bool {
        self.is_new
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    fn get_file(&self, filename: &str) -> io::Result<impl std::ops::DerefMut<Target = File> + '_> {
        let mut open_files = self.open_files.lock().unwrap();
        
        if !open_files.contains_key(filename) {
            let file_path = self.db_directory.join(filename);
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(file_path)?;
                
            open_files.insert(filename.to_string(), file);
        }
        
        struct FileGuard<'a> {
            guard: std::sync::MutexGuard<'a, HashMap<String, File>>,
            key: String,
        }
        
        impl<'a> std::ops::Deref for FileGuard<'a> {
            type Target = File;
            fn deref(&self) -> &File {
                self.guard.get(&self.key).unwrap()
            }
        }
        
        impl<'a> std::ops::DerefMut for FileGuard<'a> {
            fn deref_mut(&mut self) -> &mut File {
                self.guard.get_mut(&self.key).unwrap()
            }
        }
        
        Ok(FileGuard {
            guard: open_files,
            key: filename.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_append_and_length() {
        let temp_dir = tempdir().unwrap();
        let file_mgr = FileMgr::new(temp_dir.path(), 400).unwrap();
        
        let filename = "testfile";
        let blk1 = file_mgr.append(filename).unwrap();
        let blk2 = file_mgr.append(filename).unwrap();
        let blk3 = file_mgr.append(filename).unwrap();
        
        assert_eq!(blk1.number(), 0);
        assert_eq!(blk2.number(), 1);
        assert_eq!(blk3.number(), 2);
        
        assert_eq!(file_mgr.block_count(filename).unwrap(), 3);
    }

    #[test]
    fn test_read_write() {
        let temp_dir = tempdir().unwrap();
        let file_mgr = FileMgr::new(temp_dir.path(), 400).unwrap();
        
        let filename = "testfile";
        let blk = file_mgr.append(filename).unwrap();
        
        let mut page = Page::new(400);
        page.set_int(0, 42);
        page.set_string(4, "Hello, SimpleDB!");
        
        file_mgr.write(&blk, &page).unwrap();
        
        let mut page2 = Page::new(400);
        file_mgr.read(&blk, &mut page2).unwrap();
        
        assert_eq!(page2.get_int(0), 42);
        assert_eq!(page2.get_string(4), "Hello, SimpleDB!");
    }

    #[test]
    fn test_read_write_multiple_pages() {
        let temp_dir = tempdir().unwrap();
        let file_mgr = FileMgr::new(temp_dir.path(), 400).unwrap();

        let file_name = "testfile";
        let blk1 = file_mgr.append(file_name).unwrap();

        let mut page1 = Page::new(400);
        page1.set_int(0, 42);
        page1.set_string(100, "Hello, SimpleDB!");
        page1.set_int(200, -54574);

        file_mgr.append(file_name).unwrap();
        let blk2 = file_mgr.append(file_name).unwrap();

        let mut page2 = Page::new(400);
        page2.set_string(0, "test string");
        page2.set_int(300, 89658853);

        file_mgr.write(&blk1, &page1).unwrap();
        file_mgr.write(&blk2, &page2).unwrap();

        let mut page1_read = Page::new(400);
        file_mgr.read(&blk1, &mut page1_read).unwrap();

        assert_eq!(page1_read.get_int(0), 42);

        let mut page2_read = Page::new(400);
        file_mgr.read(&blk2, &mut page2_read).unwrap();

        assert_eq!(page2_read.get_string(0), "test string");
    }
} 