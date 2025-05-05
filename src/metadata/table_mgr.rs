use std::collections::HashMap;
use tempfile::TempDir;

use crate::{error::DbResult, record::{layout::Layout, schema::{FieldType, Schema}, table_scan::TableScan}, tx::transaction::Transaction};

pub struct TableMgr {
    tcat_layout: Layout,
    fcat_layout: Layout,
}

impl TableMgr {
    pub const MAX_NAME: usize = 16;
    
    pub fn new(is_new: bool, tx: Transaction) -> DbResult<Self> {
        let mut tcat_schema = Schema::new();
        tcat_schema.add_string_field("tblname", Self::MAX_NAME);
        tcat_schema.add_int_field("slotsize");
        let tcat_layout = Layout::new(tcat_schema.clone());

        let mut fcat_schema = Schema::new();
        fcat_schema.add_string_field("tblname", Self::MAX_NAME);
        fcat_schema.add_string_field("fldname", Self::MAX_NAME);
        fcat_schema.add_int_field("type");
        fcat_schema.add_int_field("length");
        fcat_schema.add_int_field("offset");
        let fcat_layout = Layout::new(fcat_schema.clone());

        let table_mgr = Self {
            tcat_layout,
            fcat_layout,
        };

        if is_new {
            table_mgr.create_table("tblcat", &tcat_schema, tx.clone())?;
            table_mgr.create_table("fldcat", &fcat_schema, tx.clone())?;
        }

        Ok(table_mgr)
    }

    pub fn create_table(&self, tblname: &str, sch: &Schema, tx: Transaction) -> DbResult<()> {
        let layout = Layout::new(sch.clone());
        
        {
            let mut tcat = TableScan::new(tx.clone(), "tblcat", self.tcat_layout.clone())?;
            tcat.insert();
            tcat.set_string("tblname", tblname.to_string());
            tcat.set_int("slotsize", layout.slot_size() as i32);
        }

        {
            let mut fcat = TableScan::new(tx.clone(), "fldcat", self.fcat_layout.clone())?;
            for fldname in sch.fields() {
                fcat.insert();
                fcat.set_string("tblname", tblname.to_string());
                fcat.set_string("fldname", fldname.clone());
                let type_value = match sch.field_type(&fldname).unwrap() {
                    FieldType::Integer => 0,
                    FieldType::Varchar => 1,
                };
                fcat.set_int("type", type_value);
                fcat.set_int("length", sch.length(&fldname).unwrap_or(0) as i32);
                fcat.set_int("offset", layout.offset(&fldname).unwrap_or(0) as i32);
            }
        }

        Ok(())
    }

    pub fn get_layout(&self, tblname: &str, tx: Transaction) -> DbResult<Layout> {
        let mut size = -1;
        {
            let mut tcat = TableScan::new(tx.clone(), "tblcat", self.tcat_layout.clone())?;
        
            while tcat.next()? {
                if tcat.get_string("tblname")? == tblname {
                    size = tcat.get_int("slotsize")?;
                    break;
                }
            }
        }

        let mut sch = Schema::new();
        let mut offsets = HashMap::new();
        let mut fcat = TableScan::new(tx.clone(), "fldcat", self.fcat_layout.clone())?;
        
        while fcat.next()? {
            if fcat.get_string("tblname")? == tblname {
                let fldname = fcat.get_string("fldname")?;
                let fldtype = fcat.get_int("type")?;
                let fldlen = fcat.get_int("length")?;
                let offset = fcat.get_int("offset")?;
                
                offsets.insert(fldname.clone(), offset as usize);
                let field_type = match fldtype {
                    0 => FieldType::Integer,
                    1 => FieldType::Varchar,
                    _ => panic!("Unknown field type: {}", fldtype),
                };
                sch.add_field(&fldname, field_type, fldlen as usize);
            }
        }
        
        Ok(Layout::with_offsets(sch, offsets, size as usize))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{buffer::buffer_mgr::BufferMgr, error::DbResult, log::LogMgr, metadata::table_mgr::TableMgr, record::schema::Schema, storage::file_mgr::FileMgr, tx::transaction::Transaction};
    use tempfile::TempDir;

    #[test]
    fn test_table_mgr() -> DbResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_mgr = Arc::new(FileMgr::new(temp_dir.path(), 400)?);
        let log_mgr = Arc::new(LogMgr::new(Arc::clone(&file_mgr), "testlog")?);
        let buffer_mgr = Arc::new(BufferMgr::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), 3));

        let tx: Transaction<'_> = Transaction::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), &buffer_mgr)?;
        
        let table_mgr = TableMgr::new(true, tx.clone())?;
        
        let mut test_schema = Schema::new();
        test_schema.add_int_field("id");
        test_schema.add_string_field("name", 20);
        test_schema.add_int_field("age");
        
        table_mgr.create_table("test_table", &test_schema, tx.clone())?;
        
        let layout = table_mgr.get_layout("test_table", tx.clone())?;
        
        // assert!(layout.has_field("id"));
        // assert!(layout.has_field("name"));
        // assert!(layout.has_field("age"));
                
        assert!(layout.slot_size() > 0);
        
        tx.commit()?;
        
        let tx2: Transaction<'_> = Transaction::new(Arc::clone(&file_mgr), Arc::clone(&log_mgr), &buffer_mgr)?;
        let layout2 = table_mgr.get_layout("test_table", tx2.clone())?;
        
        assert_eq!(layout.slot_size(), layout2.slot_size());
        assert_eq!(layout.schema().fields().len(), layout2.schema().fields().len());
        
        tx2.commit()?;
        Ok(())
    }
}