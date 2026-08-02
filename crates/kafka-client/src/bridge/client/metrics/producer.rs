//! Private generated-free projection of engine producer metrics.

use kafka_client_engine::EngineProducerMetrics;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ClientProducerMetrics(EngineProducerMetrics);

impl ClientProducerMetrics {
    pub(super) const fn from_engine(inner: EngineProducerMetrics) -> Self {
        Self(inner)
    }

    pub(crate) const fn active_records(self) -> usize {
        self.0.active_records()
    }

    pub(crate) const fn active_bytes(self) -> usize {
        self.0.active_bytes()
    }

    pub(crate) const fn waiting_records(self) -> usize {
        self.0.waiting_records()
    }

    pub(crate) const fn waiting_bytes(self) -> usize {
        self.0.waiting_bytes()
    }

    pub(crate) const fn prepared_batches(self) -> usize {
        self.0.prepared_batches()
    }

    pub(crate) const fn prepared_batch_bytes(self) -> usize {
        self.0.prepared_batch_bytes()
    }

    pub(crate) const fn terminal_backlog(self) -> usize {
        self.0.terminal_backlog()
    }

    pub(crate) const fn accepting(self) -> bool {
        self.0.accepting()
    }

    pub(crate) const fn healthy(self) -> bool {
        self.0.healthy()
    }
}
