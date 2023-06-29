use std::{fmt::Debug, sync::Arc};

use arrow_flight::Ticket;
use dashmap::DashMap;
use datafusion::{
    arrow::datatypes::SchemaRef,
    execution::context::SessionState,
    physical_plan::{execute_stream, ExecutionPlan, SendableRecordBatchStream},
    prelude::SessionContext,
};
use prost::bytes::Bytes;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SynapseTicket(Uuid);

impl TryFrom<Ticket> for SynapseTicket {
    type Error = crate::Error;

    fn try_from(value: Ticket) -> Result<Self, Self::Error> {
        Self::from_bytes(value.ticket)
    }
}

impl From<SynapseTicket> for Ticket {
    fn from(value: SynapseTicket) -> Self {
        Ticket::new(Bytes::from(value))
    }
}

impl From<SynapseTicket> for Bytes {
    fn from(value: SynapseTicket) -> Self {
        value.0.into_bytes().to_vec().into()
    }
}

impl SynapseTicket {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_bytes(value: Bytes) -> crate::Result<Self> {
        let bytes: &[u8; 16] = value
            .as_ref()
            .try_into()
            .map_err(|_| crate::ServerError::InvalidTicket(value.clone()))?;
        Ok(Self(Uuid::from_bytes(bytes.clone())))
    }
}

#[derive(Debug, Clone)]
pub struct SynapseTask {
    plan: Arc<dyn ExecutionPlan>,
    state: SessionState,
}

impl SynapseTask {
    async fn from_sql(query: &str, ctx: &SessionContext) -> crate::Result<Self> {
        let state = ctx.state();
        let plan = state.optimize(&state.create_logical_plan(query).await?)?;
        let plan = state.create_physical_plan(&plan).await?;
        Ok(Self { plan, state })
    }

    pub fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    pub async fn stream(&self) -> crate::Result<SendableRecordBatchStream> {
        Ok(execute_stream(self.plan.clone(), self.state.task_ctx())?)
    }

    pub fn num_rows(&self) -> Option<usize> {
        let stats = self.plan.statistics();
        if stats.is_exact {
            stats.num_rows
        } else {
            None
        }
    }

    pub fn byte_size(&self) -> Option<usize> {
        let stats = self.plan.statistics();
        if stats.is_exact {
            stats.total_byte_size
        } else {
            None
        }
    }

    pub fn is_ordered(&self) -> bool {
        self.plan.output_ordering().is_some()
    }
}

#[derive(Clone)]
pub struct TicketTracker {
    tickets: DashMap<SynapseTicket, SynapseTask>,
    ctx: SessionContext,
}

impl Debug for TicketTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TicketTracker")
            .field("tickets", &self.tickets)
            .finish_non_exhaustive()
    }
}

impl TicketTracker {
    pub fn new(ctx: SessionContext) -> Self {
        Self {
            tickets: DashMap::new(),
            ctx,
        }
    }

    pub fn get(&self, ticket: &SynapseTicket) -> Option<SynapseTask> {
        self.tickets.get(ticket).map(|task| task.value().clone())
    }

    pub fn take(&self, ticket: &SynapseTicket) -> Option<SynapseTask> {
        self.tickets.remove(ticket).map(|(_, task)| task)
    }

    pub async fn put_sql(&self, query: &str) -> crate::Result<(SynapseTicket, SynapseTask)> {
        let ticket = SynapseTicket::new();
        let task = SynapseTask::from_sql(query, &self.ctx).await?;
        self.tickets.insert(ticket, task.clone());
        Ok((ticket, task))
    }
}
