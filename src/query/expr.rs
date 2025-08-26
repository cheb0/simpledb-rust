use crate::error::DbResult;
use crate::query::{Constant, Scan};
use crate::record::Schema;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Expr {
    Constant(Constant),
    FieldName(String),
}

impl Expr {
    pub fn constant(val: Constant) -> Self {
        Expr::Constant(val)
    }

    pub fn field_name(fldname: impl Into<String>) -> Self {
        Expr::FieldName(fldname.into())
    }

    pub fn evaluate(&self, s: &mut dyn Scan) -> DbResult<Constant> {
        match self {
            Expr::Constant(val) => Ok(val.clone()),
            Expr::FieldName(fldname) => s.get_val(fldname),
        }
    }

    pub fn is_field_name(&self) -> bool {
        matches!(self, Expr::FieldName(_))
    }

    pub fn as_constant(&self) -> Option<&Constant> {
        match self {
            Expr::Constant(val) => Some(val),
            Expr::FieldName(_) => None,
        }
    }

    pub fn as_field_name(&self) -> Option<&str> {
        match self {
            Expr::Constant(_) => None,
            Expr::FieldName(fldname) => Some(fldname),
        }
    }

    pub fn applies_to(&self, sch: &Schema) -> bool {
        match self {
            Expr::Constant(_) => true,
            Expr::FieldName(fldname) => sch.has_field(fldname),
        }
    }
}

impl std::fmt::Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Constant(val) => write!(f, "{}", val.to_string()),
            Expr::FieldName(fldname) => write!(f, "{}", fldname),
        }
    }
}
