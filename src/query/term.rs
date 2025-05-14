use crate::error::DbResult;
use crate::query::{Constant, Scan};
use crate::record::schema::Schema;

use super::Expression;

/// Represents a term that compares two expressions for equality.
#[derive(Debug, Clone)]
pub struct Term {
    lhs: Expression,
    rhs: Expression,
}

impl Term {
    pub fn new(lhs: Expression, rhs: Expression) -> Self {
        Term { lhs, rhs }
    }

    pub fn is_satisfied(&self, s: &mut dyn Scan) -> DbResult<bool> {
        let lhs_val = self.lhs.evaluate(s)?;
        let rhs_val = self.rhs.evaluate(s)?;
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
