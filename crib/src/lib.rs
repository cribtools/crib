use bigtools::{BBIReadError, BigWigReadOpenError};

pub mod bigwig;
pub mod file;
pub mod object_store;

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("not supported: {0}")]
    NotSupported(String),
    #[error("object store: {0}")]
    ObjectStore(String),
    #[error("io error: {0}")]
    IoError(String),
    #[error("invalid data format: {0}")]
    InvalidDataFormat(String),
    #[error("internal error: {0}")]
    InternalError(String),
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::IoError(value.to_string())
    }
}

impl From<::object_store::Error> for Error {
    fn from(err: ::object_store::Error) -> Self {
        let mut inmost_error: &dyn std::error::Error = &err;
        while let Some(source) = inmost_error.source() {
            inmost_error = source;
        }
        Error::ObjectStore(inmost_error.to_string())
    }
}

impl From<BBIReadError> for Error {
    fn from(value: BBIReadError) -> Self {
        Error::IoError(value.to_string())
    }
}

impl From<BigWigReadOpenError> for Error {
    fn from(_: BigWigReadOpenError) -> Self {
        Error::IoError("could not open remote file".to_string())
    }
}
