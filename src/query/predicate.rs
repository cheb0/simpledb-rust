use crate::error::DbResult;
use crate::query::Scan;

use super::Term;

/// Represents a predicate that combines multiple terms with AND (conjunction).
/// A predicate is satisfied only if all its terms are satisfied.
#[derive(Debug, Clone, Default)]
pub struct Predicate {
    terms: Vec<Term>,
}

impl Predicate {
    pub fn new(term: Term) -> Self {
        Predicate {
            terms: vec![term],
        }
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