use crate::query::Scan;
use crate::record::schema::Schema;
use crate::record::layout::Layout;
use crate::record::TableScan;
use crate::tx::transaction::Transaction;
use crate::DbResult;
use super::Plan;

pub struct TablePlan {
    tblname: String,
    layout: Layout,
}

impl TablePlan {
    pub fn new(tblname: String, layout: Layout) -> DbResult<Self> {
        Ok(TablePlan {
            tblname: tblname,
            layout,
        })
    }
}

impl Plan for TablePlan {
    fn open<'tx>(&self, tx: Transaction<'tx>) -> Box<dyn Scan + 'tx> {
        Box::new(TableScan::new(tx, &self.tblname, self.layout.clone()).unwrap())
    }
    
    fn schema(&self) -> Schema {
        self.layout.schema().clone()
    }
}
