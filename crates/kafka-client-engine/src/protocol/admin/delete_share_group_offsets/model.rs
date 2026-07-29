//! Generated-free API-92 response ownership for deterministic interpretation.

use kafka_client_core::{DeleteShareGroupOffsetsBatch, DeleteShareGroupOffsetsBrokerError};

/// One bounded response normalized before it can be bound to the operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ValidatedDeleteShareGroupOffsetsResponse {
    /// Exact top-level broker rejection and its retained byte charge.
    BrokerRejected {
        error: DeleteShareGroupOffsetsBrokerError,
        retained_bytes: usize,
    },
    /// Exact caller-ordered per-topic outcomes and their retained byte charge.
    Batch {
        batch: DeleteShareGroupOffsetsBatch,
        retained_bytes: usize,
    },
}

impl ValidatedDeleteShareGroupOffsetsResponse {
    pub(crate) fn into_parts(
        self,
    ) -> (
        Result<DeleteShareGroupOffsetsBatch, DeleteShareGroupOffsetsBrokerError>,
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
