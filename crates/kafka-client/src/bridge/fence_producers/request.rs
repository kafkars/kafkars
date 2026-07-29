//! Caller-owned transactional IDs retained without conversion by the inert builder.

use super::engine::Request as EngineRequest;

pub(crate) struct FenceProducersAdminRequest {
    transactional_ids: Vec<String>,
}

impl FenceProducersAdminRequest {
    pub(crate) const fn new(transactional_ids: Vec<String>) -> Self {
        Self { transactional_ids }
    }

    pub(in crate::bridge) const fn transactional_id_count(&self) -> usize {
        self.transactional_ids.len()
    }

    pub(in crate::bridge) fn into_engine(self) -> EngineRequest {
        EngineRequest::new(self.transactional_ids)
    }
}

impl std::fmt::Debug for FenceProducersAdminRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FenceProducersAdminRequest")
            .field("transactional_ids", &self.transactional_ids)
            .finish()
    }
}
