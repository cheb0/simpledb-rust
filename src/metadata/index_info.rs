use std::alloc::Layout;

use crate::{record::Schema, tx::Transaction};

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
}