use uuid::{NoContext, Timestamp, Uuid};

use crate::Path;

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    derive_more::From,
    derive_more::AsRef,
    derive_more::Display,
)]
#[from(forward)]
#[as_ref(forward)]
pub struct TopicId(String);

impl TopicId {
    pub fn to_string(self) -> String {
        self.0
    }
}

macro_rules! impl_uuid_newtype {
    ($([$t:ident $($prefix:literal)?])+) => {
        $(
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            serde::Serialize,
            serde::Deserialize,
            derive_more::Display,
        )]
        pub struct $t(Uuid);

        impl $t {
            pub fn new() -> Self {
                Self(Uuid::new_v7(Timestamp::now(NoContext)))
            }

            impl_uuid_newtype!(@encode $t $($prefix)?);
        }
        )+
    };
    (@encode $t:ident $prefix:literal) => {
        pub(crate) fn encode_path(&self, root: &Path, ext: &str) -> Path {
            encode_uuid_to_path(self.0, root, Some($prefix), ext)
        }
    };
    (@encode $t:ident) => {
        pub(crate) fn encode_path(&self, root: &Path, ext: &str) -> Path {
            encode_uuid_to_path(self.0, root, None, ext)
        }
    };
}

impl_uuid_newtype!(
    [TransactionId "transaction"]
    [SnapshotId "snapshot"]
    [ShardId]
);

impl ShardId {
    pub(crate) fn generate_from(id: &Self) -> Self {
        Self(Uuid::new_v7(
            id.0.get_timestamp().expect("expected v7 UUID"),
        ))
    }
}

fn encode_uuid_to_path(uuid: Uuid, root: &Path, prefix: Option<&str>, ext: &str) -> Path {
    let mut buf = Uuid::encode_buffer();
    let id = uuid.hyphenated().encode_lower(&mut buf);
    let file = if let Some(prefix) = prefix {
        format!("{}-{}.{}", prefix, id, ext)
    } else {
        format!("{}.{}", id, ext)
    };
    root.join(&file)
}
