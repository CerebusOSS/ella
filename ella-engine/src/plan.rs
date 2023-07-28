use std::{fmt::Debug, sync::Arc};

use arrow_schema::SchemaRef;
use datafusion::{common::DFSchema, logical_expr::LogicalPlan, prelude::SessionContext};
use datafusion_proto::bytes::{
    logical_plan_from_bytes_with_extension_codec, logical_plan_to_bytes_with_extension_codec,
};

use crate::{codec::RemoteExtensionCodec, engine::EllaState};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Plan {
    inner: PlanInner,
    definition: Option<String>,
}

impl Plan {
    pub fn definition(&self) -> Option<String> {
        self.definition.clone()
    }

    pub fn with_definition(mut self, definition: String) -> Self {
        self.definition = Some(definition);
        self
    }

    pub fn resolve(&self, state: &EllaState) -> crate::Result<LogicalPlan> {
        self.inner.resolve(state)
    }

    pub fn stub(&self) -> &LogicalPlan {
        self.inner.stub()
    }

    pub fn as_resolved(&self) -> Option<LogicalPlan> {
        match &self.inner {
            PlanInner::Resolved(plan) => Some(plan.clone()),
            PlanInner::Stub(_) => None,
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        Ok(Self {
            inner: PlanInner::from_raw(bytes)?,
            definition: None,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.inner.to_raw()
    }

    pub fn from_plan(plan: LogicalPlan) -> Self {
        Self {
            inner: PlanInner::Resolved(plan),
            definition: None,
        }
    }

    pub fn from_stub(stub: LogicalPlan) -> Self {
        Self {
            inner: PlanInner::Stub(stub),
            definition: None,
        }
    }

    pub fn arrow_schema(&self) -> SchemaRef {
        (**self.inner.stub().schema()).clone().into()
    }

    pub fn df_schema(&self) -> &Arc<DFSchema> {
        self.inner.stub().schema()
    }

    pub fn map<F>(mut self, f: F) -> Self
    where
        F: FnOnce(LogicalPlan) -> LogicalPlan,
    {
        self.inner = match self.inner {
            PlanInner::Resolved(plan) => PlanInner::Resolved(f(plan)),
            PlanInner::Stub(plan) => PlanInner::Stub(f(plan)),
        };
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PlanInner {
    Resolved(LogicalPlan),
    Stub(LogicalPlan),
}

impl PlanInner {
    fn resolve(&self, state: &EllaState) -> crate::Result<LogicalPlan> {
        Ok(match self {
            Self::Resolved(plan) => plan.clone(),
            Self::Stub(plan) => {
                let codec = state.codec();
                let ctx = SessionContext::with_state(state.session().clone());
                let raw = logical_plan_to_bytes_with_extension_codec(plan, &codec)?;
                logical_plan_from_bytes_with_extension_codec(&raw, &ctx, &codec)?
            }
        })
    }

    fn stub(&self) -> &LogicalPlan {
        match self {
            Self::Stub(stub) => stub,
            Self::Resolved(plan) => plan,
        }
    }

    fn to_raw(&self) -> Vec<u8> {
        match self {
            Self::Resolved(plan) | Self::Stub(plan) => {
                let codec = RemoteExtensionCodec {};
                logical_plan_to_bytes_with_extension_codec(plan, &codec)
                    .unwrap()
                    .into()
            }
        }
    }

    fn from_raw(raw: &[u8]) -> crate::Result<Self> {
        let ctx = SessionContext::new();
        let codec = RemoteExtensionCodec {};
        Ok(Self::Stub(logical_plan_from_bytes_with_extension_codec(
            raw, &ctx, &codec,
        )?))
    }
}

impl serde::Serialize for PlanInner {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.to_raw())
    }
}

impl<'de> serde::Deserialize<'de> for PlanInner {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = <Vec<u8> as serde::Deserialize<'de>>::deserialize(deserializer)?;
        Self::from_raw(&raw).map_err(<D::Error as serde::de::Error>::custom)
    }
}
