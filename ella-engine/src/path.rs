use std::{fmt::Display, path::PathBuf, str::FromStr};

use object_store::path::Path as ObjPath;
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Path(Url);

impl Display for Path {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for Path {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl AsRef<Url> for Path {
    fn as_ref(&self) -> &Url {
        &self.0
    }
}

impl FromStr for Path {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(url) = Url::parse(s) {
            Ok(Self(url))
        } else {
            let mut path: PathBuf = s.parse().expect("path parsing is infallible");

            // Can't use fs::canonicalize if path doesn't exist yet
            if path.exists() {
                path = path.canonicalize()?;
            } else if !path.has_root() {
                path = std::env::current_dir()?.join(path);
            }

            match Url::from_file_path(path) {
                Ok(url) => Ok(Self(url)),
                Err(_) => Err(crate::EngineError::InvalidFilename(format!(
                    "\"{s}\" is not a valid URL or local filepath"
                ))
                .into()),
            }
        }
    }
}

impl TryFrom<&str> for Path {
    type Error = crate::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Self(Url::try_from(value)?))
    }
}

impl Path {
    pub(crate) fn as_path(&self) -> ObjPath {
        ObjPath::from(self.0.path())
    }

    pub fn join(&self, input: &str) -> Self {
        let mut out = self.clone();
        out.0.path_segments_mut().unwrap().push(input);
        out
    }

    pub fn filename(&self) -> Option<&str> {
        self.0.path_segments().and_then(|p| p.last())
    }

    pub(crate) fn store_url(&self) -> Self {
        let mut this = self.clone();
        if let Ok(mut path) = this.0.path_segments_mut() {
            path.clear();
        }
        this
    }
}
