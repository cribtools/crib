pub mod bbi;
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
    #[error("invalid file format: {0}")] // TODO: reduce variants?
    InvalidFileFormat(String),
    #[error("internal error: {0}")]
    InternalError(String),
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::IoError(value.to_string())
    }
}
