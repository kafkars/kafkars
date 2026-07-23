//! Producer-local terminal values carried by the single completion notifier.

use kafka_client_core::ProducerCompletion;

/// One terminal value whose concrete operation kind is decided by the producer.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ProducerTerminal {
    Record(ProducerCompletion),
}

impl ProducerTerminal {
    /// Wraps one deterministic record-delivery decision for engine publication.
    pub(crate) const fn record(completion: ProducerCompletion) -> Self {
        Self::Record(completion)
    }

    /// Consumes the envelope and returns its record-delivery decision.
    pub(crate) const fn into_record(self) -> ProducerCompletion {
        match self {
            Self::Record(completion) => completion,
        }
    }
}
