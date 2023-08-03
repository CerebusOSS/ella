use std::{
    borrow::{Borrow, Cow},
    fmt::Display,
    sync::atomic::{AtomicU16, Ordering},
};

use datafusion::{
    common::SchemaReference,
    sql::{ResolvedTableReference, TableReference},
};
use rand::Fill;
use uuid::{NoContext, Timestamp, Uuid};

use crate::Path;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SchemaRef<'a> {
    pub catalog: Option<Id<'a>>,
    pub schema: Id<'a>,
}

impl<'a> Display for SchemaRef<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(catalog) = &self.catalog {
            write!(f, "{}.", catalog)?;
        }
        write!(f, "{}", self.schema)
    }
}

impl<'a> SchemaRef<'a> {
    pub fn resolve(&self, default_catalog: &Id<'_>) -> SchemaId<'static> {
        SchemaId {
            catalog: self
                .catalog
                .as_ref()
                .unwrap_or(default_catalog)
                .clone()
                .into_owned(),
            schema: self.schema.clone().into_owned(),
        }
    }

    pub fn into_owned(self) -> SchemaRef<'static> {
        SchemaRef {
            catalog: self.catalog.map(|c| c.into_owned()),
            schema: self.schema.into_owned(),
        }
    }
}

impl<'a> From<&'a str> for SchemaRef<'a> {
    fn from(value: &'a str) -> Self {
        let mut iter = value.rsplit('.');
        let schema = iter.next().expect("rsplit iter should never be empty");
        let catalog = iter.next().map(Id::new);
        SchemaRef {
            schema: Id::new(schema),
            catalog,
        }
    }
}

impl From<String> for SchemaRef<'static> {
    fn from(value: String) -> Self {
        SchemaRef::from(value.as_ref()).into_owned()
    }
}

impl<'a> From<Id<'a>> for SchemaRef<'a> {
    fn from(schema: Id<'a>) -> Self {
        SchemaRef {
            catalog: None,
            schema,
        }
    }
}

impl<'a> From<SchemaId<'a>> for SchemaRef<'a> {
    fn from(value: SchemaId<'a>) -> Self {
        Self {
            schema: value.schema,
            catalog: Some(value.catalog),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TableRef<'a> {
    pub catalog: Option<Id<'a>>,
    pub schema: Option<Id<'a>>,
    pub table: Id<'a>,
}

impl<'a> Display for TableRef<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(catalog) = &self.catalog {
            write!(f, "{}.", catalog)?;
        }
        if let Some(schema) = &self.schema {
            write!(f, "{}.", schema)?;
        }
        write!(f, "{}", self.table)
    }
}

impl<'a> TableRef<'a> {
    pub fn resolve(&self, default_catalog: &Id<'_>, default_schema: &Id<'_>) -> TableId<'static> {
        TableId {
            catalog: self
                .catalog
                .as_ref()
                .unwrap_or(default_catalog)
                .clone()
                .into_owned(),
            schema: self
                .schema
                .as_ref()
                .unwrap_or(default_schema)
                .clone()
                .into_owned(),
            table: self.table.clone().into_owned(),
        }
    }

    pub fn with_catalog(mut self, catalog: impl Into<Id<'a>>) -> Self {
        self.catalog = Some(catalog.into());
        self
    }

    pub fn with_schema(mut self, schema: impl Into<Id<'a>>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    pub fn into_owned(self) -> TableRef<'static> {
        TableRef {
            catalog: self.catalog.map(Id::into_owned),
            schema: self.schema.map(Id::into_owned),
            table: self.table.into_owned(),
        }
    }
}

impl<'a> From<TableId<'a>> for TableRef<'a> {
    fn from(value: TableId<'a>) -> Self {
        Self {
            catalog: value.catalog.into(),
            schema: value.schema.into(),
            table: value.table,
        }
    }
}

impl<'a> From<&'a str> for TableRef<'a> {
    fn from(value: &'a str) -> Self {
        let mut iter = value.rsplit('.');
        let table = iter.next().expect("rsplit iter should never be empty");
        let schema = iter.next().map(Id::new);
        let catalog = iter.next().map(Id::new);
        TableRef {
            table: Id::new(table),
            schema,
            catalog,
        }
    }
}

impl From<String> for TableRef<'static> {
    fn from(value: String) -> Self {
        TableRef::from(value.as_ref()).into_owned()
    }
}

impl<'a> From<Id<'a>> for TableRef<'a> {
    fn from(table: Id<'a>) -> Self {
        TableRef {
            table,
            catalog: None,
            schema: None,
        }
    }
}

impl<'a> From<TableReference<'a>> for TableRef<'a> {
    fn from(value: TableReference<'a>) -> Self {
        match value {
            TableReference::Bare { table } => TableRef {
                table: table.into(),
                schema: None,
                catalog: None,
            },
            TableReference::Partial { schema, table } => TableRef {
                table: table.into(),
                schema: Some(schema.into()),
                catalog: None,
            },
            TableReference::Full {
                catalog,
                schema,
                table,
            } => TableRef {
                table: table.into(),
                schema: Some(schema.into()),
                catalog: Some(catalog.into()),
            },
        }
    }
}

impl<'a> From<TableRef<'a>> for TableReference<'a> {
    fn from(value: TableRef<'a>) -> Self {
        match (value.catalog, value.schema) {
            (Some(catalog), Some(schema)) => TableReference::Full {
                catalog: catalog.into(),
                schema: schema.into(),
                table: value.table.into(),
            },
            (None, Some(schema)) => TableReference::Partial {
                schema: schema.into(),
                table: value.table.into(),
            },
            (_, None) => TableReference::Bare {
                table: value.table.into(),
            },
        }
    }
}

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
pub struct Id<'a>(Cow<'a, str>);

impl<'a> Id<'a> {
    pub fn new(id: impl Into<Cow<'a, str>>) -> Self {
        Self(id.into())
    }

    pub fn into_owned(self) -> Id<'static> {
        Id(Cow::Owned(self.0.into_owned()))
    }
}

impl<'a> From<&'a Id<'static>> for Id<'a> {
    fn from(value: &'a Id<'static>) -> Self {
        Self(Cow::Borrowed(&value.0))
    }
}

impl<'a> Borrow<str> for Id<'a> {
    fn borrow(&self) -> &str {
        self.0.borrow()
    }
}

// Have to implement Into instead of From due to trait collisions
#[allow(clippy::from_over_into)]
impl<'a> Into<Cow<'a, str>> for Id<'a> {
    fn into(self) -> Cow<'a, str> {
        self.0
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    derive_more::Into,
    derive_more::From,
    derive_more::AsRef,
    derive_more::Display,
)]
#[as_ref(forward)]
pub struct CatalogId<'a>(pub Id<'a>);

impl<'a> CatalogId<'a> {
    pub fn new(id: impl Into<Id<'a>>) -> Self {
        Self(id.into())
    }

    pub fn schema(&self, schema: impl Into<Id<'a>>) -> SchemaId<'a> {
        SchemaId {
            catalog: self.clone().into(),
            schema: schema.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SchemaId<'a> {
    pub catalog: Id<'a>,
    pub schema: Id<'a>,
}

impl<'a> SchemaId<'a> {
    pub fn new(catalog: impl Into<Id<'a>>, schema: impl Into<Id<'a>>) -> Self {
        Self {
            catalog: catalog.into(),
            schema: schema.into(),
        }
    }

    pub fn table(&self, table: impl Into<Id<'a>>) -> TableId<'a> {
        TableId {
            catalog: self.catalog.clone(),
            schema: self.schema.clone(),
            table: table.into(),
        }
    }

    pub fn into_owned(self) -> SchemaId<'static> {
        SchemaId {
            catalog: self.catalog.into_owned(),
            schema: self.schema.into_owned(),
        }
    }

    pub fn parse(name: &'a str, default_catalog: Id<'a>) -> Self {
        let mut iter = name.rsplit('.');
        let schema = iter
            .next()
            .expect("split iterator should never be empty")
            .into();
        let catalog = iter.next().map_or(default_catalog, |c| c.into());
        Self { catalog, schema }
    }

    pub fn resolve(schema: SchemaReference<'a>, default_catalog: Id<'a>) -> Self {
        match schema {
            SchemaReference::Bare { schema } => Self {
                catalog: default_catalog,
                schema: schema.into(),
            },
            SchemaReference::Full { schema, catalog } => Self {
                catalog: catalog.into(),
                schema: schema.into(),
            },
        }
    }
}

impl<'a> Display for SchemaId<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.catalog, self.schema)
    }
}

impl<'a, C: Into<Id<'a>>, S: Into<Id<'a>>> From<(C, S)> for SchemaId<'a> {
    fn from((catalog, schema): (C, S)) -> Self {
        Self {
            catalog: catalog.into(),
            schema: schema.into(),
        }
    }
}

impl<'a> From<SchemaId<'a>> for (Id<'a>, Id<'a>) {
    fn from(SchemaId { catalog, schema }: SchemaId<'a>) -> Self {
        (catalog, schema)
    }
}

impl<'a> From<SchemaId<'a>> for (CatalogId<'a>, Id<'a>) {
    fn from(SchemaId { catalog, schema }: SchemaId<'a>) -> Self {
        (catalog.into(), schema)
    }
}

impl<'a> From<SchemaId<'a>> for CatalogId<'a> {
    fn from(SchemaId { catalog, .. }: SchemaId<'a>) -> Self {
        CatalogId(catalog)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TableId<'a> {
    pub catalog: Id<'a>,
    pub schema: Id<'a>,
    pub table: Id<'a>,
}

impl<'a> TableId<'a> {
    pub fn into_owned(self) -> TableId<'static> {
        TableId {
            catalog: self.catalog.into_owned(),
            schema: self.schema.into_owned(),
            table: self.table.into_owned(),
        }
    }
}

impl<'a> Display for TableId<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

impl<'a, C, S, T> From<(C, S, T)> for TableId<'a>
where
    C: Into<Id<'a>>,
    S: Into<Id<'a>>,
    T: Into<Id<'a>>,
{
    fn from((catalog, schema, table): (C, S, T)) -> Self {
        Self {
            catalog: catalog.into(),
            schema: schema.into(),
            table: table.into(),
        }
    }
}

impl<'a, T: Into<Id<'a>>> From<(SchemaId<'a>, T)> for TableId<'a> {
    fn from((schema, table): (SchemaId<'a>, T)) -> Self {
        Self {
            catalog: schema.catalog,
            schema: schema.schema,
            table: table.into(),
        }
    }
}

impl<'a> From<TableId<'a>> for (Id<'a>, Id<'a>, Id<'a>) {
    fn from(
        TableId {
            catalog,
            schema,
            table,
        }: TableId<'a>,
    ) -> Self {
        (catalog, schema, table)
    }
}

impl<'a> From<TableId<'a>> for (SchemaId<'a>, Id<'a>) {
    fn from(
        TableId {
            catalog,
            schema,
            table,
        }: TableId<'a>,
    ) -> Self {
        ((catalog, schema).into(), table)
    }
}

impl<'a> From<TableId<'a>> for SchemaId<'a> {
    fn from(
        TableId {
            catalog, schema, ..
        }: TableId<'a>,
    ) -> Self {
        SchemaId { catalog, schema }
    }
}

impl<'a> From<TableId<'a>> for TableReference<'a> {
    fn from(value: TableId<'a>) -> Self {
        Self::full(value.catalog.0, value.schema.0, value.table.0)
    }
}

impl<'a> From<TableId<'a>> for ResolvedTableReference<'a> {
    fn from(value: TableId<'a>) -> Self {
        ResolvedTableReference {
            catalog: value.catalog.0,
            schema: value.schema.0,
            table: value.table.0,
        }
    }
}

impl<'a> From<ResolvedTableReference<'a>> for TableId<'a> {
    fn from(value: ResolvedTableReference<'a>) -> Self {
        TableId {
            catalog: value.catalog.into(),
            schema: value.schema.into(),
            table: value.table.into(),
        }
    }
}

impl<'a> From<&'a TableId<'static>> for TableId<'a> {
    fn from(value: &'a TableId<'static>) -> Self {
        TableId {
            catalog: (&value.catalog).into(),
            schema: (&value.schema).into(),
            table: (&value.table).into(),
        }
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

        #[allow(clippy::new_without_default)]
        impl $t {
            pub fn new() -> Self {
                Self(new_uuid_now())
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
        let millis = decode_timestamp_millis(&id.0);
        Self(new_uuid(millis))
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

// TODO: replace UUID logic once v7 is stabilized
// See: [CerebusOSS/ella#11](https://github.com/CerebusOSS/ella/issues/11)
// Current implementation is taken from here: https://github.com/uuid-rs/uuid/blob/main/src/timestamp.rs

fn new_uuid_now() -> Uuid {
    let ts = Timestamp::now(NoContext);
    let (secs, nanos) = ts.to_unix();
    let millis = (secs * 1000).saturating_add(nanos as u64 / 1_000_000);

    new_uuid(millis)
}

fn new_uuid(millis: u64) -> Uuid {
    static COUNTER: AtomicU16 = AtomicU16::new(0);

    let counter = COUNTER.fetch_add(1, Ordering::SeqCst);
    let mut bytes = [0u8; 10];
    bytes[..2].copy_from_slice(&counter.to_be_bytes());
    bytes[2..].try_fill(&mut rand::thread_rng()).unwrap();

    encode_timestamp_millis(millis, &bytes)
}

fn encode_timestamp_millis(millis: u64, bytes: &[u8; 10]) -> Uuid {
    let millis_high = ((millis >> 16) & 0xFFFF_FFFF) as u32;
    let millis_low = (millis & 0xFFFF) as u16;
    let random_and_version = (bytes[1] as u16 | ((bytes[0] as u16) << 8) & 0x0FFF) | (0x7 << 12);

    let d4 = [
        (bytes[2] & 0x3F) | 0x80,
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
    ];

    Uuid::from_fields(millis_high, millis_low, random_and_version, &d4)
}

fn decode_timestamp_millis(uuid: &Uuid) -> u64 {
    let bytes = uuid.as_bytes();
    let millis: u64 = (bytes[0] as u64) << 40
        | (bytes[1] as u64) << 32
        | (bytes[2] as u64) << 24
        | (bytes[3] as u64) << 16
        | (bytes[4] as u64) << 8
        | (bytes[5] as u64);
    millis
}
