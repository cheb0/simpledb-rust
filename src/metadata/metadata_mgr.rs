use std::{collections::HashMap, sync::Arc};

use crate::{
    error::DbResult,
    metadata::{IndexInfo, IndexMgr},
    record::{Layout, Schema},
    tx::Transaction,
};

use super::TableMgr;

pub struct MetadataMgr {
    table_mgr: Arc<TableMgr>,
    index_mgr: Arc<IndexMgr>,
}

impl MetadataMgr {
    pub fn new(table_mgr: Arc<TableMgr>, index_mgr: Arc<IndexMgr>) -> DbResult<Self> {
        Ok(Self {
            table_mgr,
            index_mgr,
        })
    }

    pub fn create_table(&self, tblname: &str, schema: &Schema, tx: Transaction) -> DbResult<()> {
        self.table_mgr.create_table(tblname, schema, tx)
    }

    pub fn get_layout(&self, tblname: &str, tx: Transaction) -> DbResult<Layout> {
        self.table_mgr.get_layout(tblname, tx)
    }

    pub fn create_index(
        &self,
        index_name: &str,
        table_name: &str,
        field_name: &str,
        tx: Transaction<'_>,
    ) -> DbResult<()> {
        self.index_mgr
            .create_index(index_name, table_name, field_name, tx)
    }

    pub fn get_index_info<'tx>(
        &self,
        table_name: &str,
        tx: Transaction<'tx>,
    ) -> DbResult<HashMap<String, IndexInfo>> {
        self.index_mgr.get_index_info(table_name, tx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{record::Schema, utils::testing_utils::temp_db};

    #[test]
    fn test_metadata_mgr() -> DbResult<()> {
        let db = temp_db()?;

        let tx = db.new_tx()?;

        let mut table_schema = Schema::new();
        table_schema.add_int_field("id");
        table_schema.add_string_field("name", 20);
        table_schema.add_int_field("age");
        let layout = Layout::new(table_schema.clone());

        db.metadata_mgr()
            .create_table("test_table", &table_schema, tx.clone())?;

        tx.commit()?;
        drop(tx);

        let db2 = db.reopen()?;
        let tx = db2.new_tx()?;

        let layout2 = db2.metadata_mgr().get_layout("test_table", tx)?;

        assert_eq!(layout.slot_size(), layout.slot_size());
        assert_eq!(
            layout.schema().fields().len(),
            layout2.schema().fields().len()
        );

        Ok(())
    }
}
