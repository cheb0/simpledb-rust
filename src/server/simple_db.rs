use std::path::Path;
use std::sync::Arc;

use crate::buffer::BufferMgr;
use crate::error::DbResult;
use crate::log::LogMgr;
use crate::metadata::MetadataMgr;

use crate::plan::Planner;
use crate::storage::FileMgr;
use crate::tx::Transaction;

use super::Config;

pub struct SimpleDB<'a> {
    file_mgr: Arc<FileMgr>,
    log_mgr: Arc<LogMgr>,
    buffer_mgr: &'a BufferMgr,
    planner: Planner,
    metadata_mgr: Arc<MetadataMgr>,
}

impl<'a> SimpleDB<'a> {
    pub fn with_config(config: Config) -> DbResult<Self> {
        let file_mgr = Arc::new(FileMgr::new(&config.db_directory, config.block_size)?);
        let log_mgr = Arc::new(LogMgr::new(Arc::clone(&file_mgr), config.log_file_path().to_str().unwrap())?);
        
        // TODO fix this
        let buffer_mgr = Box::leak(Box::new(BufferMgr::new(
            Arc::clone(&file_mgr),
            Arc::clone(&log_mgr),
            config.buffer_capacity,
        )));


        // TODO recover if is_new
        let tx = Transaction::new(
            Arc::clone(&file_mgr),
            Arc::clone(&log_mgr),
            &buffer_mgr,
        )?;
        
        let md_mgr = Arc::new(MetadataMgr::new(file_mgr.is_new(), tx.clone())?);
        let planner = Planner::new(Arc::clone(&md_mgr));
        tx.commit()?;
    
        Ok(Self {
            file_mgr,
            log_mgr,
            buffer_mgr: buffer_mgr,
            metadata_mgr: md_mgr,
            planner: planner,
        })
    }

    pub fn new<P: AsRef<Path>>(db_directory: P) -> DbResult<Self> {
        Self::with_config(Config::new(db_directory))
    }

    pub fn load_metadata(&mut self) -> DbResult<()> {
        let tx = Transaction::new(
            Arc::clone(&self.file_mgr),
            Arc::clone(&self.log_mgr),
            &self.buffer_mgr,
        )?;
        
        let md_mgr = MetadataMgr::new(false, tx.clone())?;
        tx.commit()?;
        
        self.metadata_mgr = Arc::new(md_mgr);
        Ok(())
    }

    pub fn new_tx(&'a self) -> DbResult<Transaction<'a>> {
        Transaction::new(
            Arc::clone(&self.file_mgr),
            Arc::clone(&self.log_mgr),
            &self.buffer_mgr,
        )
    }

    pub fn file_mgr(&self) -> Arc<FileMgr> {
        Arc::clone(&self.file_mgr)
    }

    pub fn log_mgr(&self) -> Arc<LogMgr> {
        Arc::clone(&self.log_mgr)
    }

    pub fn metadata_mgr(&self) -> Arc<MetadataMgr> {
        self.metadata_mgr.clone()
    }

    pub fn planner(&self) -> &Planner {
        &self.planner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Schema;
    use tempfile::TempDir;

    #[test]
    fn test_simple_db() -> DbResult<()> {
        let temp_dir = TempDir::new().unwrap();
        
        let db = SimpleDB::with_config(
            Config::new(temp_dir.path())
                .block_size(400)
                .buffer_capacity(5)
                .log_file("testlog")
        )?;

        let md_mgr = db.metadata_mgr();
        
        let tx = db.new_tx()?;
        
        let mut test_schema = Schema::new();
        test_schema.add_int_field("id");
        test_schema.add_string_field("name", 20);
        
        md_mgr.create_table("test_table", &test_schema, tx.clone())?;
        
        let layout = md_mgr.get_layout("test_table", tx.clone())?;
        assert!(layout.slot_size() > 0);
        
        tx.commit()?;
        
        Ok(())
    }
} 