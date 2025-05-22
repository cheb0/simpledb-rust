use crate::query::Scan;
use crate::record::schema::Schema;
use crate::record::layout::Layout;
use crate::record::TableScan;
use crate::tx::transaction::Transaction;
use crate::metadata::metadata_mgr::MetadataMgr;
use crate::DbResult;
use super::Plan;

pub struct TablePlan<'tx> {
    tblname: String,
    tx: Transaction<'tx>,
    layout: Layout,
}

impl<'tx> TablePlan<'tx> {
    pub fn new(tx: Transaction<'tx>, tblname: &str, md: &MetadataMgr) -> DbResult<Self> {
        let layout = md.get_layout(tblname, tx.clone())?;
        Ok(TablePlan {
            tblname: tblname.to_string(),
            tx: tx.clone(),
            layout,
        })
    }
}

impl<'tx> Plan<'tx> for TablePlan<'tx> {
    fn open(& self) -> Box<dyn Scan + 'tx> {
        Box::new(TableScan::new(self.tx.clone(), &self.tblname, self.layout.clone()).unwrap())
    }
    
    fn schema(&self) -> Schema {
        self.layout.schema().clone()
    }
}
