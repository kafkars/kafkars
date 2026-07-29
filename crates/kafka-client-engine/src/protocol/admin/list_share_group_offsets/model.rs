//! Generated-free API-90 response ownership for deterministic interpretation.

use kafka_client_core::{ListShareGroupOffsetsBatch, ListShareGroupOffsetsBrokerError};

/// One bounded response normalized before it can be bound to the operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ValidatedListShareGroupOffsetsResponse {
    BrokerRejected {
        error: ListShareGroupOffsetsBrokerError,
        retained_bytes: usize,
    },
    Batch {
        batch: ListShareGroupOffsetsBatch,
        retained_bytes: usize,
    },
}

impl ValidatedListShareGroupOffsetsResponse {
    pub(crate) fn into_parts(
        self,
    ) -> (
        Result<ListShareGroupOffsetsBatch, ListShareGroupOffsetsBrokerError>,
        usize,
    ) {
        match self {
            Self::BrokerRejected {
                error,
                retained_bytes,
            } => (Err(error), retained_bytes),
            Self::Batch {
                batch,
                retained_bytes,
            } => (Ok(batch), retained_bytes),
        }
    }
}
