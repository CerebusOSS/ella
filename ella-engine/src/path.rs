use std::{fmt::Display, str::FromStr};

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
        Ok(Self(Url::parse(s)?))
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
