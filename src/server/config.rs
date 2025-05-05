use std::path::{Path, PathBuf};

pub struct Config {
    pub db_directory: PathBuf,
    pub block_size: usize,
    pub buffer_capacity: usize,
    pub log_file_name: String,
}

impl Config {
    pub fn new<P: AsRef<Path>>(db_directory: P) -> Self {
        Self {
            db_directory: db_directory.as_ref().to_path_buf(),
            block_size: 4096,
            buffer_capacity: 8,
            log_file_name: "simpledb.log".to_string(),
        }
    }

    pub fn block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size;
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
        self.db_directory.join(&self.log_file_name)
    }
} 