use std::fmt;

#[derive(Debug, PartialEq)]
pub enum ParseError {
    Incomplete,
    Invalid(&'static str),
    Io,
}

impl From<&'static str> for ParseError {
    fn from(msg: &'static str) -> Self {
        ParseError::Invalid(msg)
    }
}

impl From<std::io::Error> for ParseError {
    fn from(_: std::io::Error) -> Self {
        ParseError::Io
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::Incomplete => write!(f, "Incomplete packet"),
            ParseError::Invalid(msg) => write!(f, "Invalid packet: {}", msg),
            ParseError::Io => write!(f, "I/O error"),
        }
    }
}

impl std::error::Error for ParseError {}
