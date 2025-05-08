use crate::{error::DbResult, record::schema::Schema, record::layout::Layout, tx::transaction::Transaction};
use super::table_mgr::TableMgr;

pub struct MetadataMgr {
    table_mgr: TableMgr,
}

impl MetadataMgr {
    pub fn new(is_new: bool, tx: Transaction) -> DbResult<Self> {
        let table_mgr = TableMgr::new(is_new, tx)?;
        Ok(Self { table_mgr })
    }

    pub fn create_table(&self, tblname: &str, schema: &Schema, tx: Transaction) -> DbResult<()> {
        self.table_mgr.create_table(tblname, schema, tx)
    }

    pub fn get_layout(&self, tblname: &str, tx: Transaction) -> DbResult<Layout> {
        self.table_mgr.get_layout(tblname, tx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{buffer::buffer_mgr::BufferMgr, log::LogMgr, record::schema::Schema, storage::file_mgr::FileMgr};
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_metadata_mgr() -> DbResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_mgr = Arc::new(FileMgr::new(temp_dir.path(), 400)?);
        let log_mgr = Arc::new(LogMgr::new(Arc::clone(&file_mgr), "testlog")?);
        let buffer_mgr = Arc::new(BufferMgr::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), 3));

        let tx = Transaction::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), &buffer_mgr)?;
        
        let md_mgr = MetadataMgr::new(true, tx.clone())?;
        
        let mut test_schema = Schema::new();
        test_schema.add_int_field("id");
        test_schema.add_string_field("name", 20);
        test_schema.add_int_field("age");
        
        md_mgr.create_table("test_table", &test_schema, tx.clone())?;
        
        let layout = md_mgr.get_layout("test_table", tx.clone())?;
        
        assert!(layout.slot_size() > 0);
        
        tx.commit()?;
        
        let tx2 = Transaction::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), &buffer_mgr)?;
        let layout2 = md_mgr.get_layout("test_table", tx2.clone())?;
        
        assert_eq!(layout.slot_size(), layout2.slot_size());
        assert_eq!(layout.schema().fields().len(), layout2.schema().fields().len());
        
        tx2.commit()?;
        Ok(())
    }
} 