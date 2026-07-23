//! Producer-owned record, flush, and execution-loss terminal values.

use kafka_client_core::ProducerCompletion;

/// One producer terminal value retained by the shared completion registry.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ProducerTerminal {
    Record(ProducerCompletion),
    FlushCompleted,
    ExecutionUnavailable,
}

impl ProducerTerminal {
    /// Wraps one record delivery terminal at the registry boundary.
    pub(crate) const fn record(completion: ProducerCompletion) -> Self {
        Self::Record(completion)
    }

    /// Creates a successful flush terminal.
    pub(crate) const fn flush_completed() -> Self {
        Self::FlushCompleted
    }

    /// Creates a type-neutral terminal after permanent host execution loss.
    pub(crate) const fn execution_unavailable() -> Self {
        Self::ExecutionUnavailable
    }
}
