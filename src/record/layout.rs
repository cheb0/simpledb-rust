use super::schema::{FieldType, Schema};
use crate::storage::Page;
use std::collections::HashMap;

/// Description of the structure of a record.
/// It contains the name, type, length and offset of
/// each field of the table.
#[derive(Debug, Clone)]
pub struct Layout {
    schema: Schema,
    offsets: HashMap<String, usize>,
    slot_size: usize,
}

impl Layout {
    pub fn new(schema: Schema) -> Self {
        let mut offsets = HashMap::new();
        let mut pos = std::mem::size_of::<i32>();

        for field_name in schema.fields() {
            offsets.insert(field_name.clone(), pos);
            pos += Self::length_in_bytes(&schema, field_name);
        }

        Layout {
            schema,
            offsets,
            slot_size: pos,
        }
    }

    pub fn with_offsets(schema: Schema, offsets: HashMap<String, usize>, slot_size: usize) -> Self {
        Layout {
            schema,
            offsets,
            slot_size,
        }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn offset(&self, field_name: &str) -> Option<usize> {
        self.offsets.get(field_name).copied()
    }

    pub fn slot_size(&self) -> usize {
        self.slot_size
    }

    fn length_in_bytes(schema: &Schema, field_name: &str) -> usize {
        match schema.field_type(field_name).expect("Field not found") {
            FieldType::Integer => std::mem::size_of::<i32>(),
            FieldType::Varchar => {
                Page::max_length(schema.length(field_name).expect("Field length not found"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_layout_basic() {
        let mut schema = Schema::new();
        schema.add_int_field("id");
        schema.add_string_field("name", 20);

        let layout = Layout::new(schema);

        assert_eq!(layout.offset("id"), Some(4));

        let name_offset = layout.offset("name").unwrap();
        assert!(name_offset > 4);

        assert!(layout.slot_size() > name_offset);
    }
}
