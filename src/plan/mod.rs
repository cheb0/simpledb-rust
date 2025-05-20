pub mod table_plan;

use crate::record::schema::Schema;
use crate::query::scan::Scan;

pub trait Plan {

    fn open(&self) -> Box<dyn Scan>;
    
    fn schema(&self) -> Schema;
}
