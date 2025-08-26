use crate::error::DbResult;
use crate::query::{Constant, Scan};
use crate::record::Schema;

use super::Term;

/// Represents a predicate that combines multiple terms with AND (conjunction).
/// A predicate is satisfied only if all its terms are satisfied.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct Predicate {
    terms: Vec<Term>,
}

impl Predicate {
    pub fn new(term: Term) -> Self {
        Predicate { terms: vec![term] }
    }

    pub fn conjoin_with(mut self, other: Predicate) -> Self {
        self.terms.extend(other.terms);
        self
    }

    pub fn with_term(mut self, term: Term) -> Self {
        self.terms.push(term);
        self
    }

    pub fn is_satisfied(&self, s: &mut dyn Scan) -> DbResult<bool> {
        for term in &self.terms {
            if !term.is_satisfied(s)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Returns the first constant that equates with the specified field name.
    /// Returns None if no term equates the field with a constant.
    pub fn equates_with_constant(&self, fldname: &str) -> Option<&Constant> {
        for term in &self.terms {
            if let Some(constant) = term.equates_with_constant(fldname) {
                return Some(constant);
            }
        }
        None
    }

    /// Creates a new predicate containing only the terms that apply to the given schema.
    /// Returns None if no terms apply to the schema.
    pub fn select_sub_pred(&self, sch: &Schema) -> Option<Predicate> {
        let mut result = Predicate::default();

        for term in &self.terms {
            if term.applies_to(sch) {
                result.terms.push(term.clone());
            }
        }

        if result.terms.is_empty() {
            None
        } else {
            Some(result)
        }
    }
}

impl std::fmt::Display for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.terms.is_empty() {
            return write!(f, "");
        }

        let mut iter = self.terms.iter();
        write!(f, "{}", iter.next().unwrap())?;

        for term in iter {
            write!(f, " and {}", term)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{Constant, Expr, Term};

    #[test]
    fn test_equates_with_constant() {
        let term = Term::new(Expr::field_name("age"), Expr::constant(Constant::int(25)));
        let predicate = Predicate::new(term);

        assert_eq!(
            predicate.equates_with_constant("age"),
            Some(&Constant::int(25))
        );
        assert_eq!(predicate.equates_with_constant("name"), None);

        let age_term = Term::new(Expr::field_name("age"), Expr::constant(Constant::int(25)));
        let name_term = Term::new(
            Expr::field_name("name"),
            Expr::constant(Constant::string("Alice")),
        );
        let predicate = Predicate::new(age_term).with_term(name_term);

        assert_eq!(
            predicate.equates_with_constant("age"),
            Some(&Constant::int(25))
        );
        assert_eq!(
            predicate.equates_with_constant("name"),
            Some(&Constant::string("Alice"))
        );
        assert_eq!(predicate.equates_with_constant("salary"), None);

        let term1 = Term::new(Expr::field_name("age"), Expr::field_name("salary"));
        let term2 = Term::new(
            Expr::constant(Constant::int(100)),
            Expr::constant(Constant::int(200)),
        );
        let predicate = Predicate::new(term1).with_term(term2);

        assert_eq!(predicate.equates_with_constant("age"), None);
        assert_eq!(predicate.equates_with_constant("salary"), None);
        assert_eq!(predicate.equates_with_constant("name"), None);

        let empty_predicate = Predicate::default();
        assert_eq!(empty_predicate.equates_with_constant("age"), None);
    }

    #[test]
    fn test_select_sub_pred() {
        use crate::record::Schema;

        let mut schema = Schema::new();
        schema.add_int_field("id");
        schema.add_string_field("name", 20);
        schema.add_int_field("age");

        // Create a predicate with terms: "id=1 and name='Alice' and age=25"
        let id_term = Term::new(Expr::field_name("id"), Expr::constant(Constant::int(1)));
        let name_term = Term::new(
            Expr::field_name("name"),
            Expr::constant(Constant::string("Alice")),
        );
        let age_term = Term::new(Expr::field_name("age"), Expr::constant(Constant::int(25)));
        let predicate = Predicate::new(id_term)
            .with_term(name_term)
            .with_term(age_term);

        let sub_pred = predicate.select_sub_pred(&schema);
        assert!(sub_pred.is_some());
        let sub_pred = sub_pred.unwrap();
        assert_eq!(sub_pred.terms.len(), 3);

        let mut partial_schema = Schema::new();
        partial_schema.add_int_field("id");
        partial_schema.add_string_field("name", 20);

        // only id and name terms should apply
        let sub_pred = predicate.select_sub_pred(&partial_schema);
        assert!(sub_pred.is_some());
        let sub_pred = sub_pred.unwrap();
        assert_eq!(sub_pred.terms.len(), 2);

        let mut age_schema = Schema::new();
        age_schema.add_int_field("age");

        let sub_pred = predicate.select_sub_pred(&age_schema);
        assert!(sub_pred.is_some());
        let sub_pred = sub_pred.unwrap();
        assert_eq!(sub_pred.terms.len(), 1);

        let mut no_match_schema = Schema::new();
        no_match_schema.add_int_field("salary");
        no_match_schema.add_string_field("department", 30);

        let sub_pred = predicate.select_sub_pred(&no_match_schema);
        assert!(sub_pred.is_none());

        let empty_predicate = Predicate::default();
        let sub_pred = empty_predicate.select_sub_pred(&schema);
        assert!(sub_pred.is_none());
    }
}
