use crate::index::BTreeIndex;
use crate::metadata::IndexInfo;
use crate::plan::{Plan, TablePlan};
use crate::query::Constant;
use crate::query::{IndexSelectScan, Scan};
use crate::record::{TableScan, schema::Schema};
use crate::tx::Transaction;

pub struct IndexSelectPlan {
    plan: TablePlan,
    index_info: IndexInfo,
    search_value: Constant,
}

impl IndexSelectPlan {
    pub fn new(plan: TablePlan, index_info: IndexInfo, search_value: Constant) -> Self {
        IndexSelectPlan {
            plan,
            index_info,
            search_value,
        }
    }
}

impl Plan for IndexSelectPlan {
    fn open<'tx>(&self, tx: Transaction<'tx>) -> Box<dyn Scan + 'tx> {
        let index_layout =
            IndexInfo::create_idx_layout(&self.index_info.field_name(), &self.plan.schema());
        let index =
            BTreeIndex::new(tx.clone(), &self.index_info.index_name(), index_layout).unwrap();
        let scan = TableScan::new(
            tx.clone(),
            self.plan.table_name(),
            self.plan.table_layout().clone(),
        )
        .unwrap();
        Box::new(IndexSelectScan::new(scan, Box::new(index), self.search_value.clone()).unwrap())
    }

    fn schema(&self) -> Schema {
        self.plan.schema()
    }
}
