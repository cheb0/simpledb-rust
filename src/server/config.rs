use std::path::{Path, PathBuf};

/// Configuration for file-based storage manager
#[derive(Clone)]
pub struct FileStorageMgrConfig {
    pub db_directory: PathBuf,
    pub block_size: usize,
}

impl FileStorageMgrConfig {
    pub fn new<P: AsRef<Path>>(db_directory: P) -> Self {
        Self {
            db_directory: db_directory.as_ref().to_path_buf(),
            block_size: 4096,
        }
    }

    pub fn block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size;
        self
    }
}

/// Configuration for in-memory storage manager
#[derive(Clone)]
pub struct MemStorageMgrConfig {
    pub block_size: usize,
}

impl MemStorageMgrConfig {
    pub fn new() -> Self {
        Self { block_size: 4096 }
    }

    pub fn block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size;
        self
    }
}

/// Configuration for different storage manager types
#[derive(Clone)]
pub enum StorageMgrConfig {
    File(FileStorageMgrConfig),
    Mem(MemStorageMgrConfig),
}

impl StorageMgrConfig {
    pub fn file<P: AsRef<Path>>(db_directory: P) -> Self {
        StorageMgrConfig::File(FileStorageMgrConfig::new(db_directory))
    }

    pub fn mem() -> Self {
        StorageMgrConfig::Mem(MemStorageMgrConfig::new())
    }

    /// Get the block size from the configuration
    pub fn block_size(&self) -> usize {
        match self {
            StorageMgrConfig::File(config) => config.block_size,
            StorageMgrConfig::Mem(config) => config.block_size,
        }
    }

    /// Get the database directory (only applicable for file-based storage)
    pub fn db_directory(&self) -> Option<&Path> {
        match self {
            StorageMgrConfig::File(config) => Some(&config.db_directory),
            StorageMgrConfig::Mem(_) => None,
        }
    }
}

#[derive(Clone)]
pub struct Config {
    pub storage_mgr: StorageMgrConfig,
    pub buffer_capacity: usize,
    pub log_file_name: String,
}

impl Config {
    pub fn new(storage_mgr: StorageMgrConfig) -> Self {
        Self {
            storage_mgr,
            buffer_capacity: 8,
            log_file_name: "simpledb.log".to_string(),
        }
    }

    /// Create a new config with file-based storage
    pub fn file<P: AsRef<Path>>(db_directory: P) -> Self {
        Self::new(StorageMgrConfig::file(db_directory))
    }

    /// Create a new config with in-memory storage
    pub fn mem() -> Self {
        Self::new(StorageMgrConfig::mem())
    }

    pub fn block_size(mut self, block_size: usize) -> Self {
        self.storage_mgr = match self.storage_mgr {
            StorageMgrConfig::File(config) => StorageMgrConfig::File(config.block_size(block_size)),
            StorageMgrConfig::Mem(config) => StorageMgrConfig::Mem(config.block_size(block_size)),
        };
        self
    }

    pub fn buffer_capacity(mut self, buffer_capacity: usize) -> Self {
        self.buffer_capacity = buffer_capacity;
        self
    }

    pub fn log_file(mut self, log_file: impl Into<String>) -> Self {
        self.log_file_name = log_file.into();
        self
    }

    pub fn log_file_path(&self) -> PathBuf {
        match &self.storage_mgr {
            StorageMgrConfig::File(config) => config.db_directory.join(&self.log_file_name),
            StorageMgrConfig::Mem(_) => PathBuf::from(&self.log_file_name), // For mem storage, just use the filename
        }
    }
}
