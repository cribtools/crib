pub mod bigwig;
pub mod file;

#[derive(thiserror::Error, Debug)]
pub enum Error {
     #[error("not supported: {0}")]
    NotSupported(String),
}