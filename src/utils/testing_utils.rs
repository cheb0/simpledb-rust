use std::{ops::Deref, sync::Arc};

use tempfile::TempDir;

use crate::{log::LogMgr, server::Config, storage::FileMgr, tx::Transaction, DbResult, SimpleDB};

const TEST_PAGE_SIZE: usize = 400;

/// DB newtype which carries an instance of DB as well as TempDir. TempDir must be dropped after 
/// DB is dropped.
pub struct TempSimpleDB<'a> {
    db: Option<SimpleDB<'a>>,
    dir: TempDir,
}

impl<'a> Deref for TempSimpleDB<'a> {
    type Target = SimpleDB<'a>;

    fn deref(&self) -> &Self::Target {
        return &self.db.as_ref().unwrap();
    }
}

// Takes out of Option<SimpleDB> which means db is destroyed before temp_dir
impl<'a> Drop for TempSimpleDB<'a> {
    fn drop(&mut self) {
        // TODO check drop order
        self.db.take();
    }
}

pub fn temp_db<'a>() -> DbResult<TempSimpleDB<'a>> {
    return temp_db_with_cfg(|cfg| cfg);
}

pub fn temp_db_with_cfg<'a>(mut cfg_updater: impl FnMut(Config) -> Config) -> DbResult<TempSimpleDB<'a>> {
    let temp_dir = TempDir::new().unwrap();
    let mut cfg = Config::new(temp_dir.path());
    cfg = cfg.block_size(TEST_PAGE_SIZE);
    cfg = cfg_updater(cfg);

    // let file_mgr = Arc::new(FileMgr::new(temp_dir.path().to_path_buf(), TEST_PAGE_SIZE)?);
    // let log_mgr = Arc::new(LogMgr::new(Arc::clone(&file_mgr), "testlog")?);
    // let buffer_mgr = BufferMgr::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), 8);

    let db = SimpleDB::with_config(cfg)?;
    return Ok(TempSimpleDB{ db: Some(db), dir: temp_dir});
}