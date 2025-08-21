pub mod block_id;
pub mod page;

pub use block_id::BlockId;
pub use page::Page;

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::error::DbResult;

/// Trait for file management operations.
/// This allows for different implementations (e.g., basic file system, in-memory, etc.)
/// Must be thread safe and support interrior mutability.
pub trait StorageMgr: Send + Sync {
    /// Reads a block from disk into the provided page.
    fn read(&self, blk: &BlockId, page: &mut Page) -> io::Result<()>;
    
    /// Writes a page to the specified block on disk.
    fn write(&self, blk: &BlockId, page: &Page) -> io::Result<()>;
    
    /// Appends a new block to the end of the specified file and returns its BlockId.
    fn append(&self, filename: &str) -> io::Result<BlockId>;
    
    /// Returns the number of blocks in the specified file.
    fn block_cnt(&self, filename: &str) -> io::Result<i32>;
    
    /// Returns whether this is a new database.
    fn is_new(&self) -> bool;
    
    /// Returns the block size.
    fn block_size(&self) -> usize;
}

/// Basic implementation of FileStorageMgr that uses the file system.
pub struct FileStorageMgr {
    db_directory: PathBuf,
    block_size: usize,
    is_new: bool,
    open_files: Mutex<HashMap<String, File>>,
}

impl FileStorageMgr {
    pub fn new<P: AsRef<Path>>(db_directory: P, block_size: usize) -> DbResult<Self> {
        let db_path = db_directory.as_ref().to_path_buf();
        
        let is_new = if db_path.exists() {
            let mut entries = fs::read_dir(&db_path)?;
            entries.next().is_none()
        } else {
            true
        };

        if !db_path.exists() {
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

        Ok(FileStorageMgr {
            db_directory: db_path,
            block_size,
            is_new,
            open_files: Mutex::new(HashMap::new()),
        })
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

impl StorageMgr for FileStorageMgr {
    fn read(&self, blk: &BlockId, page: &mut Page) -> io::Result<()> {
        let mut file = self.get_file(&blk.file_name())?;
        let pos = blk.number() as u64 * self.block_size as u64;
        file.seek(SeekFrom::Start(pos))?;
        
        let buffer = page.contents_mut();
        file.read_exact(buffer)?;
        
        Ok(())
    }

    fn write(&self, blk: &BlockId, page: &Page) -> io::Result<()> {
        let mut file = self.get_file(&blk.file_name())?;
        let pos = blk.number() as u64 * self.block_size as u64;
        file.seek(SeekFrom::Start(pos))?;
        
        // Write the page's buffer to disk
        file.write_all(page.contents())?;
        file.flush()?;
        
        Ok(())
    }

    fn append(&self, filename: &str) -> io::Result<BlockId> {
        let new_block_num = self.block_cnt(filename)?;
        let blk = BlockId::new(filename.to_string(), new_block_num);
        
        let mut file = self.get_file(&blk.file_name())?;
        let pos = blk.number() as u64 * self.block_size as u64;
        file.seek(SeekFrom::Start(pos))?;
        
        let zeros = vec![0; self.block_size];
        file.write_all(&zeros)?;
        file.flush()?;
        
        Ok(blk)
    }

    fn block_cnt(&self, filename: &str) -> io::Result<i32> {
        let file = self.get_file(filename)?;
        let file_size = file.metadata()?.len();
        Ok((file_size / self.block_size as u64) as i32)
    }

    fn is_new(&self) -> bool {
        self.is_new
    }

    fn block_size(&self) -> usize {
        self.block_size
    }
}

/// In-memory implementation of StorageMgr for testing and temporary storage.
pub struct MemStorageMgr {
    block_size: usize,
    files: Mutex<HashMap<String, Vec<Vec<u8>>>>,
}

impl MemStorageMgr {
    pub fn new(block_size: usize) -> Self {
        MemStorageMgr {
            block_size,
            files: Mutex::new(HashMap::new()),
        }
    }
}

impl StorageMgr for MemStorageMgr {
    fn read(&self, blk: &BlockId, page: &mut Page) -> io::Result<()> {
        let files = self.files.lock().unwrap();
        let blocks = files.get(blk.file_name())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "File not found"))?;
        
        let block_num = blk.number() as usize;
        if block_num >= blocks.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Block number out of range"));
        }
        
        let block_data = &blocks[block_num];
        let buffer = page.contents_mut();
        
        let block_size = block_data.len();
        // Copy data from memory block to page buffer
        if block_size != buffer.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Page size and block size do not match"));
        }
        buffer[..block_size].copy_from_slice(&block_data[..block_size]);
        Ok(())
    }

    fn write(&self, blk: &BlockId, page: &Page) -> io::Result<()> {
        let mut files = self.files.lock().unwrap();
        let blocks = files.get_mut(blk.file_name())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "File not found"))?;
        
        let block_num = blk.number() as usize;
        
        if block_num >= blocks.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Block number out of range"));
        }
        
        // Copy data from page buffer to memory block
        let block = &mut blocks[block_num];
        let page_data = page.contents();
        let block_size = block.len();
        if block_size != page_data.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Page size and block size do not match"));
        }
        block[..block_size].copy_from_slice(&page_data[..block_size]);
        
        Ok(())
    }

    fn append(&self, filename: &str) -> io::Result<BlockId> {
        let mut files = self.files.lock().unwrap();
        let blocks = files.entry(filename.to_string()).or_insert_with(Vec::new);
        
        let new_block_num = blocks.len() as i32;
        let new_block = vec![0; self.block_size];
        blocks.push(new_block);
        
        Ok(BlockId::new(filename.to_string(), new_block_num))
    }

    fn block_cnt(&self, filename: &str) -> io::Result<i32> {
        let files = self.files.lock().unwrap();
        let file_blocks = files.get(filename);
        Ok(file_blocks.map(|blocks| blocks.len() as i32).unwrap_or(0))
    }

    fn is_new(&self) -> bool {
        true
    }

    fn block_size(&self) -> usize {
        self.block_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_append_and_length() {
        let temp_dir = tempdir().unwrap();
        let storage_mgr = FileStorageMgr::new(temp_dir.path(), 400).unwrap();
        
        let filename = "testfile";
        let blk1 = storage_mgr.append(filename).unwrap();
        let blk2 = storage_mgr.append(filename).unwrap();
        let blk3 = storage_mgr.append(filename).unwrap();
        
        assert_eq!(blk1.number(), 0);
        assert_eq!(blk2.number(), 1);
        assert_eq!(blk3.number(), 2);
        
        assert_eq!(storage_mgr.block_cnt(filename).unwrap(), 3);
    }

    #[test]
    fn test_read_write() {
        let temp_dir = tempdir().unwrap();
        let storage_mgr = FileStorageMgr::new(temp_dir.path(), 400).unwrap();
        
        let filename = "testfile";
        let blk = storage_mgr.append(filename).unwrap();
        
        let mut page = Page::new(400);
        page.set_int(0, 42);
        page.set_string(4, "Hello, SimpleDB!");
        
        storage_mgr.write(&blk, &page).unwrap();
        
        let mut page2 = Page::new(400);
        storage_mgr.read(&blk, &mut page2).unwrap();
        
        assert_eq!(page2.get_int(0), 42);
        assert_eq!(page2.get_string(4), "Hello, SimpleDB!");
    }

    #[test]
    fn test_read_write_multiple_pages() {
        let temp_dir = tempdir().unwrap();
        let storage_mgr = FileStorageMgr::new(temp_dir.path(), 400).unwrap();

        let file_name = "testfile";
        let blk1 = storage_mgr.append(file_name).unwrap();

        let mut page1 = Page::new(400);
        page1.set_int(0, 42);
        page1.set_string(100, "Hello, SimpleDB!");
        page1.set_int(200, -54574);

        storage_mgr.append(file_name).unwrap();
        let blk2 = storage_mgr.append(file_name).unwrap();

        let mut page2 = Page::new(400);
        page2.set_string(0, "test string");
        page2.set_int(300, 89658853);

        storage_mgr.write(&blk1, &page1).unwrap();
        storage_mgr.write(&blk2, &page2).unwrap();

        let mut page1_read = Page::new(400);
        storage_mgr.read(&blk1, &mut page1_read).unwrap();

        assert_eq!(page1_read.get_int(0), 42);

        let mut page2_read = Page::new(400);
        storage_mgr.read(&blk2, &mut page2_read).unwrap();

        assert_eq!(page2_read.get_string(0), "test string");
    }

    #[test]
    fn test_storage_mgr_trait() {
        let temp_dir = tempdir().unwrap();
        let storage_mgr: Box<dyn StorageMgr> = Box::new(FileStorageMgr::new(temp_dir.path(), 400).unwrap());
        
        let filename = "testfile";
        let blk = storage_mgr.append(filename).unwrap();
        
        let mut page = Page::new(400);
        page.set_int(0, 123);
        page.set_string(4, "Trait test");
        
        storage_mgr.write(&blk, &page).unwrap();
        
        let mut page2 = Page::new(400);
        storage_mgr.read(&blk, &mut page2).unwrap();
        
        assert_eq!(page2.get_int(0), 123);
        assert_eq!(page2.get_string(4), "Trait test");
    }

    #[test]
    fn test_mem_storage_mgr_basic() {
        let storage_mgr = MemStorageMgr::new(400);
        
        let filename = "testfile";
        let blk1 = storage_mgr.append(filename).unwrap();
        let blk2 = storage_mgr.append(filename).unwrap();
        let blk3 = storage_mgr.append(filename).unwrap();
        
        assert_eq!(blk1.number(), 0);
        assert_eq!(blk2.number(), 1);
        assert_eq!(blk3.number(), 2);
        
        assert_eq!(storage_mgr.block_cnt(filename).unwrap(), 3);
    }

    #[test]
    fn test_mem_storage_mgr_read_write() {
        let storage_mgr = MemStorageMgr::new(400);
        
        let filename = "testfile";
        let blk = storage_mgr.append(filename).unwrap();
        
        let mut page = Page::new(400);
        page.set_int(0, 42);
        page.set_string(4, "Hello, Memory!");
        
        storage_mgr.write(&blk, &page).unwrap();
        
        let mut page2 = Page::new(400);
        storage_mgr.read(&blk, &mut page2).unwrap();
        
        assert_eq!(page2.get_int(0), 42);
        assert_eq!(page2.get_string(4), "Hello, Memory!");
    }

    #[test]
    fn test_mem_storage_mgr_multiple_files() {
        let storage_mgr = MemStorageMgr::new(400);
        
        // Create two different files
        let file1 = "file1";
        let file2 = "file2";
        
        let blk1 = storage_mgr.append(file1).unwrap();
        let blk2 = storage_mgr.append(file2).unwrap();
        
        let mut page1 = Page::new(400);
        page1.set_int(0, 100);
        
        let mut page2 = Page::new(400);
        page2.set_int(0, 200);
        
        storage_mgr.write(&blk1, &page1).unwrap();
        storage_mgr.write(&blk2, &page2).unwrap();
        
        let mut read_page1 = Page::new(400);
        let mut read_page2 = Page::new(400);
        
        storage_mgr.read(&blk1, &mut read_page1).unwrap();
        storage_mgr.read(&blk2, &mut read_page2).unwrap();
        
        assert_eq!(read_page1.get_int(0), 100);
        assert_eq!(read_page2.get_int(0), 200);
        
        assert_eq!(storage_mgr.block_cnt(file1).unwrap(), 1);
        assert_eq!(storage_mgr.block_cnt(file2).unwrap(), 1);
    }

    #[test]
    fn test_mem_storage_mgr_trait_object() {
        let storage_mgr: Box<dyn StorageMgr> = Box::new(MemStorageMgr::new(400));
        
        let filename = "testfile";
        let blk = storage_mgr.append(filename).unwrap();
        
        let mut page = Page::new(400);
        page.set_int(0, 123);
        page.set_string(4, "Memory trait test");
        
        storage_mgr.write(&blk, &page).unwrap();
        
        let mut page2 = Page::new(400);
        storage_mgr.read(&blk, &mut page2).unwrap();
        
        assert_eq!(page2.get_int(0), 123);
        assert_eq!(page2.get_string(4), "Memory trait test");
    }

    #[test]
    fn test_file_storage_mgr_new_database_detection() -> DbResult<()> {
        use tempfile::TempDir;
        
        let temp_dir = TempDir::new()?;
        let non_existent_path = temp_dir.path().join("non_existent_db");
        
        let storage_mgr = FileStorageMgr::new(&non_existent_path, 400)?;
        assert!(storage_mgr.is_new(), "Non-existent directory should be detected as new database");
        
        let empty_db_path = temp_dir.path().join("empty_db");
        fs::create_dir_all(&empty_db_path)?;
        
        let storage_mgr = FileStorageMgr::new(&empty_db_path, 400)?;
        assert!(storage_mgr.is_new(), "Empty directory should be detected as new database");
        
        let existing_db_path = temp_dir.path().join("existing_db");
        fs::create_dir_all(&existing_db_path)?;
        
        let dummy_file = existing_db_path.join("dummy.txt");
        fs::write(&dummy_file, "dummy content")?;
        
        let storage_mgr = FileStorageMgr::new(&existing_db_path, 400)?;
        assert!(!storage_mgr.is_new(), "Directory with files should NOT be detected as new database");
        
        Ok(())
    }
}