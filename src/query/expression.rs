use crate::error::DbResult;
use crate::query::{Constant, Scan};
use crate::record::schema::Schema;

#[derive(Debug, Clone)]
pub enum Expression {
    Constant(Constant),
    FieldName(String),
}

impl Expression {
    pub fn with_constant(val: Constant) -> Self {
        Expression::Constant(val)
    }

    pub fn with_field_name(fldname: impl Into<String>) -> Self {
        Expression::FieldName(fldname.into())
    }

    pub fn evaluate(&self, s: &mut dyn Scan) -> DbResult<Constant> {
        match self {
            Expression::Constant(val) => Ok(val.clone()),
            Expression::FieldName(fldname) => s.get_val(fldname),
        }
    }

    pub fn is_field_name(&self) -> bool {
        matches!(self, Expression::FieldName(_))
    }

    pub fn as_constant(&self) -> Option<&Constant> {
        match self {
            Expression::Constant(val) => Some(val),
            Expression::FieldName(_) => None,
        }
    }

    pub fn as_field_name(&self) -> Option<&str> {
        match self {
            Expression::Constant(_) => None,
            Expression::FieldName(fldname) => Some(fldname),
        }
    }

    pub fn applies_to(&self, sch: &Schema) -> bool {
        match self {
            Expression::Constant(_) => true,
            Expression::FieldName(fldname) => sch.has_field(fldname),
        }
    }
}

impl std::fmt::Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expression::Constant(val) => write!(f, "{}", val.to_string()),
            Expression::FieldName(fldname) => write!(f, "{}", fldname),
        }
    }
}