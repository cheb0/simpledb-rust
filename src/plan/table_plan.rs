use crate::record::schema::Schema;
use crate::record::layout::Layout;
use crate::tx::transaction::Transaction;
use crate::metadata::metadata_mgr::MetadataMgr;
use crate::query::scan::Scan;
use crate::query::scan::table_scan::TableScan;
use super::Plan;

pub struct TablePlan {
    tblname: String,
    tx: Transaction,
    layout: Layout,
}

impl TablePlan {
    pub fn new(tx: Transaction, tblname: String, md: &MetadataMgr) -> Self {
        let layout = md.get_layout(&tblname, &tx);
        TablePlan {
            tblname,
            tx,
            layout,
        }
    }
}

impl Plan for TablePlan {
    fn open(&self) -> Box<dyn Scan> {
        Box::new(TableScan::new(self.tx.clone(), self.tblname.clone(), self.layout.clone()))
    }
    
    fn schema(&self) -> Schema {
        self.layout.schema()
    }
}
