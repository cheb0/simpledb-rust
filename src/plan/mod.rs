pub mod index_select_plan;
pub mod planner;
pub mod project_plan;
pub mod select_plan;
pub mod table_plan;

pub use index_select_plan::IndexSelectPlan;
pub use planner::Planner;
pub use table_plan::TablePlan;

use crate::query::scan::Scan;
use crate::record::schema::Schema;
use crate::tx::Transaction;

pub trait Plan {
    fn open<'tx>(&self, tx: Transaction<'tx>) -> Box<dyn Scan + 'tx>;

    fn schema(&self) -> Schema;
}
