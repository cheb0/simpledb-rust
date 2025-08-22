#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Constant {
    Int(i32),
    String(String),
}

impl Constant {
    pub fn int(val: i32) -> Self {
        Constant::Int(val)
    }

    pub fn string(val: impl Into<String>) -> Self {
        Constant::String(val.into())
    }

    pub fn is_integer(&self) -> bool {
        matches!(self, Constant::Int(_))
    }

    pub fn is_string(&self) -> bool {
        matches!(self, Constant::String(_))
    }

    pub fn as_integer(&self) -> i32 {
        match self {
            Constant::Int(i) => *i,
            _ => panic!("Not an integer constant"),
        }
    }

    pub fn as_string(&self) -> &str {
        match self {
            Constant::String(s) => s,
            _ => panic!("Not a string constant"),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            Constant::Int(i) => i.to_string(),
            Constant::String(s) => s.clone(),
        }
    }

    /// Compare this constant with another constant.
    pub fn compare_to(&self, other: &Constant) -> std::cmp::Ordering {
        match (self, other) {
            (Constant::Int(a), Constant::Int(b)) => a.cmp(b),
            (Constant::String(a), Constant::String(b)) => a.cmp(b),
            _ => panic!("Cannot compare different constant types"),
        }
    }
}

impl From<i32> for Constant {
    fn from(value: i32) -> Self {
        Constant::Int(value)
    }
}

impl From<String> for Constant {
    fn from(value: String) -> Self {
        Constant::String(value)
    }
}

impl PartialOrd for Constant {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Constant {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.compare_to(other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constant_comparison() {
        // Integer comparisons
        assert_eq!(
            Constant::int(5).compare_to(&Constant::int(3)),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            Constant::int(3).compare_to(&Constant::int(5)),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            Constant::int(5).compare_to(&Constant::int(5)),
            std::cmp::Ordering::Equal
        );

        // String comparisons
        assert_eq!(
            Constant::string("abc").compare_to(&Constant::string("aaa")),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            Constant::string("aaa").compare_to(&Constant::string("abc")),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            Constant::string("abc").compare_to(&Constant::string("abc")),
            std::cmp::Ordering::Equal
        );

        // Test Ord trait integration
        let mut constants = vec![Constant::int(3), Constant::int(1), Constant::int(2)];
        constants.sort();
        assert_eq!(
            constants,
            vec![Constant::int(1), Constant::int(2), Constant::int(3),]
        );
    }

    #[test]
    #[should_panic(expected = "Cannot compare different constant types")]
    fn test_constant_comparison_panic() {
        let _ = Constant::int(5).compare_to(&Constant::string("hello"));
    }
}
