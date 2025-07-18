#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Constant {
    Integer(i32),
    String(String),
}

impl Constant {
    pub fn integer(val: i32) -> Self {
        Constant::Integer(val)
    }
    
    pub fn string(val: impl Into<String>) -> Self {
        Constant::String(val.into())
    }
    
    pub fn is_integer(&self) -> bool {
        matches!(self, Constant::Integer(_))
    }
    
    pub fn is_string(&self) -> bool {
        matches!(self, Constant::String(_))
    }
    
    pub fn as_integer(&self) -> i32 {
        match self {
            Constant::Integer(i) => *i,
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
            Constant::Integer(i) => i.to_string(),
            Constant::String(s) => s.clone(),
        }
    }
} 