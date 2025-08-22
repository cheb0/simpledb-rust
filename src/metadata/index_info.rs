use crate::{record::{schema::FieldType, Layout, Schema}, tx::Transaction};

pub struct IndexInfo<'tx> {
    index_name: String,
    field_name: String,
    tx: Transaction<'tx>,
    table_schema: Schema,
    index_layout: Layout,
}

impl<'a> IndexInfo<'a> {
    pub const BLOCK_NUM_FIELD: &'static str = "block"; //   the block number
    pub const ID_FIELD: &'static str = "id"; //  the record id (slot number)
    pub const DATA_FIELD: &'static str = "dataval"; //  the data field

    pub fn new(index_name: String, field_name: String, tx: Transaction<'a>, table_schema: Schema) -> IndexInfo<'a> {
        let index_layout = IndexInfo::create_idx_layout(&field_name, &table_schema);
        Self {
            index_name,
            field_name,
            tx,
            table_schema,
            index_layout
        }
    }

    pub fn index_name(&self) -> &str {
        &self.index_name
    }

    pub fn field_name(&self) -> &str {
        &self.field_name
    }

    pub fn create_idx_layout(field_name: &str, table_schema: &Schema) -> Layout {
        let mut schema = Schema::new();
        schema.add_int_field(IndexInfo::BLOCK_NUM_FIELD);
        schema.add_int_field(IndexInfo::ID_FIELD);
        
        match table_schema.field_type(field_name).unwrap() {
            FieldType::Integer => {
                schema.add_int_field(IndexInfo::DATA_FIELD);
            },
            FieldType::Varchar => {
                let field_len = table_schema.length(field_name).unwrap();
                schema.add_string_field(IndexInfo::DATA_FIELD, field_len);
            },
        }
        Layout::new(schema)
    }
}