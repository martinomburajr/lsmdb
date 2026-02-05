use std::{
    fmt::{self, Display, Formatter},
    path::PathBuf,
};

pub trait Encode {
    fn encode(&self) -> Vec<u8>;
}

pub trait Decode: Sized {
    fn decode(bytes: &[u8]) -> Result<Self, DBError>;
}

pub const ERR_CONFIG_EMPTY_KEY: &'static str = "empty key";

#[derive(Debug)]
pub enum DBError {
    Io {
        op: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
    Corruption {
        what: &'static str,
        path: PathBuf,
        offset: u64,
    },
    WAL {
        what: &'static str,
        err: Option<Box <dyn std::error::Error + Send + Sync + 'static>>,
    },
    Codec {
        context: String,
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },
    InvalidConfig {
        what: &'static str,
    },
}

impl std::error::Error for DBError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

impl Display for DBError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DBError::Io { op, path, source } => {
                write!(f, "op: {op:?} - path: {path:?} - source: {source:?}")
            }
            DBError::Corruption { what, path, offset } => {
                write!(f, "what: {what:?} - path: {path:?} - offset: {offset:?}")
            }
            DBError::Codec { context, source } => {
                write!(f, "context: {context:?} - source: {source:?}")
            }
            DBError::InvalidConfig { what } => {
                write!(f, "what: {what}")
            }
            DBError::WAL { what, err } => {
                write!(f, "what: {what:?} - err: {err:?}")
            }
        }
    }
}
