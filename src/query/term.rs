use crate::error::DbResult;
use crate::query::{Constant, Scan};
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

    /// Returns the constant value if this term equates the specified field with a constant.
    /// Returns None if the term doesn't equate the field with a constant.
    pub fn equates_with_constant(&self, fldname: &str) -> Option<&Constant> {
        if let Some(lhs_field) = self.lhs.as_field_name()
            && lhs_field == fldname
            && !self.rhs.is_field_name()
        {
            return self.rhs.as_constant();
        }

        if let Some(rhs_field) = self.rhs.as_field_name()
            && rhs_field == fldname
            && !self.lhs.is_field_name()
        {
            return self.lhs.as_constant();
        }

        None
    }
}

impl std::fmt::Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}={}", self.lhs, self.rhs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{Constant, Expr};

    #[test]
    fn test_equates_with_constant() {
        let term = Term::new(Expr::field_name("age"), Expr::constant(Constant::int(25)));
        assert_eq!(term.equates_with_constant("age"), Some(&Constant::int(25)));
        assert_eq!(term.equates_with_constant("name"), None);

        let term = Term::new(Expr::constant(Constant::int(25)), Expr::field_name("age"));
        assert_eq!(term.equates_with_constant("age"), Some(&Constant::int(25)));
        assert_eq!(term.equates_with_constant("name"), None);

        let term = Term::new(Expr::field_name("age"), Expr::field_name("name"));
        assert_eq!(term.equates_with_constant("age"), None);
        assert_eq!(term.equates_with_constant("name"), None);

        let term = Term::new(
            Expr::constant(Constant::int(25)),
            Expr::constant(Constant::int(30)),
        );
        assert_eq!(term.equates_with_constant("age"), None);

        let term = Term::new(
            Expr::field_name("name"),
            Expr::constant(Constant::string("Alice")),
        );
        assert_eq!(
            term.equates_with_constant("name"),
            Some(&Constant::string("Alice"))
        );

        let term = Term::new(
            Expr::constant(Constant::string("Bob")),
            Expr::field_name("name"),
        );
        assert_eq!(
            term.equates_with_constant("name"),
            Some(&Constant::string("Bob"))
        );
    }
}
