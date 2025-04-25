use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FieldType {
    Integer,
    Varchar,
}

#[derive(Debug, Clone)]
struct FieldInfo {
    field_type: FieldType,
    length: usize,
}

#[derive(Debug, Clone)]
pub struct Schema {
    fields: Vec<String>,
    info: HashMap<String, FieldInfo>,
}

impl Schema {
    pub fn new() -> Self {
        Schema {
            fields: Vec::new(),
            info: HashMap::new(),
        }
    }

    pub fn add_field(&mut self, field_name: String, field_type: FieldType, length: usize) {
        self.fields.push(field_name.clone());
        self.info.insert(field_name, FieldInfo { field_type, length });
    }

    pub fn add_int_field(&mut self, field_name: String) {
        self.add_field(field_name, FieldType::Integer, 0);
    }

    pub fn add_string_field(&mut self, field_name: String, length: usize) {
        self.add_field(field_name, FieldType::Varchar, length);
    }

    pub fn add_from_schema(&mut self, field_name: String, other: &Schema) {
        let field_info = other.info.get(&field_name)
            .expect("Field not found in schema");
        self.add_field(
            field_name,
            field_info.field_type,
            field_info.length,
        );
    }

    pub fn add_all(&mut self, other: &Schema) {
        for field_name in &other.fields {
            self.add_from_schema(field_name.clone(), other);
        }
    }

    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    pub fn has_field(&self, field_name: &str) -> bool {
        self.fields.iter().any(|f| f == field_name)
    }

    pub fn field_type(&self, field_name: &str) -> Option<FieldType> {
        self.info.get(field_name).map(|info| info.field_type)
    }

    pub fn length(&self, field_name: &str) -> Option<usize> {
        self.info.get(field_name).map(|info| info.length)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_basic() {
        let mut schema = Schema::new();
        
        schema.add_int_field("id".to_string());
        schema.add_string_field("name".to_string(), 20);

        assert!(schema.has_field("id"));
        assert!(schema.has_field("name"));
        assert!(!schema.has_field("age"));

        assert_eq!(schema.field_type("id"), Some(FieldType::Integer));
        assert_eq!(schema.field_type("name"), Some(FieldType::Varchar));
        assert_eq!(schema.length("name"), Some(20));
    }

    #[test]
    fn test_schema_add_all() {
        let mut schema1 = Schema::new();
        schema1.add_int_field("id".to_string());
        schema1.add_string_field("name".to_string(), 20);

        let mut schema2 = Schema::new();
        schema2.add_all(&schema1);

        assert_eq!(schema2.field_type("id"), Some(FieldType::Integer));
        assert_eq!(schema2.field_type("name"), Some(FieldType::Varchar));
        assert_eq!(schema2.length("name"), Some(20));
    }
}
