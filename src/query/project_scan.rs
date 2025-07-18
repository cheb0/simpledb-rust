use crate::query::scan::Scan;
use crate::DbResult;
use crate::error::DbError;

use super::Constant;

/// The scan class corresponding to the project relational algebra operator.
/// All methods except hasField delegate their work to the underlying scan.
pub struct ProjectScan<'a> {
    scan: Box<dyn Scan + 'a>,
    fields: Vec<String>,
}

impl<'a> ProjectScan<'a> {
    pub fn new(s: Box<dyn Scan + 'a>, field_names: Vec<String>) -> Self {
        ProjectScan { scan: s, fields: field_names }
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.fields.iter().any(|f| f == field_name)
    }
}

impl<'a> Scan for ProjectScan<'a> {
    fn before_first(&mut self) -> DbResult<()> {
        self.scan.before_first()
    }

    fn next(&mut self) -> DbResult<bool> {
        self.scan.next()
    }

    fn get_int(&mut self, field_name: &str) -> DbResult<i32> {
        if self.has_field(field_name) {
            self.scan.get_int(field_name)
        } else {
            Err(DbError::FieldNotFound(field_name.to_string()))
        }
    }

    fn get_string(&mut self, field_name: &str) -> DbResult<String> {
        if self.has_field(field_name) {
            self.scan.get_string(field_name)
        } else {
            Err(DbError::FieldNotFound(field_name.to_string()))
        }
    }

    fn get_val(&mut self, field_name: &str) -> DbResult<Constant> {
        if self.has_field(field_name) {
            self.scan.get_val(field_name)
        } else {
            Err(DbError::FieldNotFound(field_name.to_string()))
        }
    }
    
    fn has_field(&self, field_name: &str) -> bool {
        self.fields.iter().any(|f| f == field_name)
    }

    fn close(&mut self) {
        self.scan.close();
    }
}