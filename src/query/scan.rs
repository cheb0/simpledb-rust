use crate::record::schema::Schema;
use crate::error::DbResult;
use crate::query::constant::Constant;

/// The `Scan` trait provides an interface for iterating through records in a database.
pub trait Scan {
    /// Position the scan before the first record.
    fn before_first(&mut self) -> DbResult<()>;
    
    fn next(&mut self) -> DbResult<bool>;
    
    fn get_int(&mut self, field_name: &str) -> DbResult<i32>;
    fn get_string(&mut self, field_name: &str) -> DbResult<String>;
    fn get_val(&mut self, field_name: &str) -> DbResult<Constant>;
    fn has_field(&self, field_name: &str) -> bool;
    
    fn close(&mut self);
    fn schema(&self) -> &Schema;
} 