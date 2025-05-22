pub mod table_plan;
pub mod project_plan;

use crate::record::schema::Schema;
use crate::query::scan::Scan;

pub trait Plan<'a> {
    
    fn open(&self) -> Box<dyn Scan + 'a>;

    fn schema(&self) -> Schema;
}