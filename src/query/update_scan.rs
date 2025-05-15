use crate::error::DbResult;
use crate::query::Constant;
use crate::query::Scan;
use crate::record::RowId;

/// The `UpdateScan` trait extends `Scan` with methods for modifying records.
pub trait UpdateScan: Scan {
    fn set_val(&mut self, field_name: &str, val: Constant) -> DbResult<()>;
    fn set_int(&mut self, field_name: &str, val: i32) -> DbResult<()>;
    fn set_string(&mut self, field_name: &str, val: &str) -> DbResult<()>;
    fn insert(&mut self) -> DbResult<()>;
    fn delete(&mut self) -> DbResult<()>;
    fn get_rid(&self) -> DbResult<RowId>;
    fn move_to_rid(&mut self, rid: RowId) -> DbResult<()>;
} 