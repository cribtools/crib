use std::path::PathBuf;
use url::Url;

#[derive(Clone, Debug)]
pub enum FileLocation {
    S3(Url),
    Http(Url),
    Local(PathBuf),
}

impl From<&str> for FileLocation {

    fn from(value: &str) -> Self {
        let maybe_url: Result<Url, _> = value.try_into();
        if let Ok(url) = maybe_url {
            match url.scheme() {
              "s3" => FileLocation::S3(url),
              _ => FileLocation::Http(url)
            }
        } else {
            FileLocation::Local(PathBuf::from(value))
        }
    }
}