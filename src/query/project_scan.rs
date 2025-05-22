use crate::query::scan::Scan;
use crate::DbResult;
use crate::error::DbError;

use super::Constant;

pub struct ProjectScan<'a> {
    s: Box<dyn Scan + 'a>,
    fields: Vec<String>,
}

impl<'a> ProjectScan<'a> {
    pub fn new(s: Box<dyn Scan + 'a>, fieldlist: Vec<String>) -> Self {
        ProjectScan { s, fields: fieldlist }
    }

    fn has_field(&self, fldname: &str) -> bool {
        self.fields.iter().any(|f| f == fldname)
    }
}

impl<'a> Scan for ProjectScan<'a> {
    fn before_first(&mut self) -> DbResult<()> {
        self.s.before_first()
    }

    fn next(&mut self) -> DbResult<bool> {
        self.s.next()
    }

    fn get_int(&mut self, fldname: &str) -> DbResult<i32> {
        if self.has_field(fldname) {
            self.s.get_int(fldname)
        } else {
            Err(DbError::FieldNotFound(fldname.to_string()))
        }
    }

    fn get_string(&mut self, fldname: &str) -> DbResult<String> {
        if self.has_field(fldname) {
            self.s.get_string(fldname)
        } else {
            Err(DbError::FieldNotFound(fldname.to_string()))
        }
    }

    fn get_val(&mut self, fldname: &str) -> DbResult<Constant> {
        if self.has_field(fldname) {
            self.s.get_val(fldname)
        } else {
            Err(DbError::FieldNotFound(fldname.to_string()))
        }
    }
    
    fn has_field(&self, field_name: &str) -> bool {
        self.fields.iter().any(|f| f == field_name)
    }

    fn close(&mut self) {
        self.s.close();
    }
}