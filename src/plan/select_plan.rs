use crate::plan::Plan;
use crate::query::{Predicate, Scan, SelectScan};
use crate::record::schema::Schema;
use crate::tx::Transaction;

pub struct SelectPlan {
    plan: Box<dyn Plan>,
    predicate: Predicate,
}

impl SelectPlan {
    pub fn new(plan: Box<dyn Plan>, predicate: Predicate) -> Self {
        SelectPlan { plan, predicate }
    }
}

impl Plan for SelectPlan {
    fn open<'tx>(&self, tx: Transaction<'tx>) -> Box<dyn Scan + 'tx> {
        let scan = self.plan.open(tx);
        Box::new(SelectScan::new(scan, self.predicate.clone()))
    }

    fn schema(&self) -> Schema {
        self.plan.schema()
    }
}
