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