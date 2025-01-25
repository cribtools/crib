pub mod bigwig;
pub mod file;
pub mod view;

#[derive(thiserror::Error, Debug)]
pub enum Error {
     #[error("not supported: {0}")]
    NotSupported(String),
    #[error("object store: {0}")]
    ObjectStore(String),
    #[error("io error: {0}")]
    IoError(String),
    #[error("internal error: {0}")]
    InternalError(String),
}