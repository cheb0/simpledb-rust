use crate::error::DbResult;
use crate::query::Scan;
use crate::record::Schema;

use super::Expr;

/// Represents a term that compares two expressions for equality.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Term {
    lhs: Expr,
    rhs: Expr,
}

impl Term {
    pub fn new(lhs: Expr, rhs: Expr) -> Self {
        Term { lhs, rhs }
    }

    pub fn is_satisfied(&self, scan: &mut dyn Scan) -> DbResult<bool> {
        let lhs_val = self.lhs.evaluate(scan)?;
        let rhs_val = self.rhs.evaluate(scan)?;
        Ok(lhs_val == rhs_val)
    }

    pub fn applies_to(&self, sch: &Schema) -> bool {
        self.lhs.applies_to(sch) && self.rhs.applies_to(sch)
    }
}

impl std::fmt::Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}={}", self.lhs, self.rhs)
    }
}
