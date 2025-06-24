pub mod table_plan;
pub mod project_plan;
pub mod planner;
pub mod select_plan;

pub use planner::Planner;

use crate::record::schema::Schema;
use crate::query::scan::Scan;
use crate::tx::Transaction;

pub trait Plan {
    
    fn open<'tx>(&self, tx: Transaction<'tx>) -> Box<dyn Scan + 'tx>;

    fn schema(&self) -> Schema;
}