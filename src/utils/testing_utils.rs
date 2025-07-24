use std::{ops::Deref};

use tempfile::TempDir;

use crate::{server::Config, DbResult, SimpleDB};

const TEST_PAGE_SIZE: usize = 400;

/// DB newtype which carries an instance of DB as well as TempDir. TempDir must be dropped after 
/// DB is dropped.
pub struct TempSimpleDB<'a> {
    db: Option<SimpleDB<'a>>,
    _dir: TempDir,
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

    let db = SimpleDB::with_config(cfg)?;
    return Ok(TempSimpleDB{ db: Some(db), _dir: temp_dir});
}