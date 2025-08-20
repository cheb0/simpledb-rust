use std::{collections::HashMap, sync::Arc};

use crate::{metadata::{IndexInfo, TableMgr}, query::{Scan, UpdateScan}, record::{Layout, Schema, TableScan}, tx::Transaction, DbResult};

pub struct IndexMgr {
    layout: Layout,
    table_mgr: Arc<TableMgr>
}

impl IndexMgr {
    pub const INDEX_TABLE: &'static str = "idxcat";

    pub const INDEX_NAME: &'static str = "indexname";
    pub const TABLE_NAME: &'static str = "tablename";
    pub const FIELD_NAME: &'static str = "fieldname";

    pub fn new(is_new_db: bool, table_mgr: Arc<TableMgr>, tx: Transaction<'_>) -> DbResult<IndexMgr> {
        if is_new_db {
            let mut schema = Schema::new();
            schema.add_string_field(IndexMgr::INDEX_NAME, TableMgr::MAX_NAME);
            schema.add_string_field(IndexMgr::TABLE_NAME, TableMgr::MAX_NAME);
            schema.add_string_field(IndexMgr::FIELD_NAME, TableMgr::MAX_NAME);
            table_mgr.create_table(IndexMgr::INDEX_TABLE, &schema, tx.clone())?;
        }
        let layout = table_mgr.get_layout(IndexMgr::INDEX_TABLE, tx)?;
        let table_mgr = Arc::clone(&table_mgr);
        Ok(IndexMgr { layout: layout, table_mgr: table_mgr })
    }

    pub fn create_index(&self, index_name: &str, table_name: &str, field_name: &str, tx: Transaction<'_>) -> DbResult<()> {
        let mut scan = TableScan::new(tx.clone(), IndexMgr::TABLE_NAME, self.layout.clone())?;
        scan.insert()?;
        scan.set_string(IndexMgr::INDEX_NAME, index_name)?;
        scan.set_string(IndexMgr::TABLE_NAME, table_name)?;
        scan.set_string(IndexMgr::FIELD_NAME, field_name)?;
        Ok(())
    }

    pub fn get_index_info<'tx>(&self, table_name: &str, tx: Transaction<'tx>) -> DbResult<HashMap<String, IndexInfo<'tx>>> {
        let mut scan = TableScan::new(tx.clone(), IndexMgr::TABLE_NAME, self.layout.clone())?;
        let mut result = HashMap::new();
        while scan.next()? {
            if scan.get_string(IndexMgr::TABLE_NAME)? == table_name {
                let index_name = scan.get_string(IndexMgr::INDEX_NAME)?;
                let field_name = scan.get_string(IndexMgr::FIELD_NAME)?;
                let table_layout = self.table_mgr.get_layout(table_name, tx.clone())?;
                let index_info = IndexInfo::new(
                    index_name,
                    field_name.clone(),
                    tx.clone(),
                    table_layout.schema().clone()
                );
                result.insert(field_name, index_info);
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use crate::{utils::testing_utils::temp_db, DbResult};

    #[test]
    fn test_zero_indexes() -> DbResult<()> {
        let db = temp_db()?;
        let tx = db.new_tx()?;

        let indices = db.metadata_mgr().get_index_info("test_table", tx)?;

        assert!(indices.is_empty());
        Ok(())
    }

    #[test]
    fn test_create_index() -> DbResult<()> {
        let db = temp_db()?;
        {
            let tx = db.new_tx()?;
            db.metadata_mgr().create_index("test_index", "test_table", "name", tx.clone())?;
            db.metadata_mgr().create_index("test_index", "test_table", "age", tx.clone())?;
            tx.commit()?;
        }

        {
            let tx = db.new_tx()?;
            let indices = db.metadata_mgr().get_index_info("test_table", tx.clone())?;
            tx.commit()?;
        }
        Ok(())
    }
}