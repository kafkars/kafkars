//! Generated-free API-91 response ownership for deterministic interpretation.

use kafka_client_core::{AlterShareGroupOffsetsBatch, AlterShareGroupOffsetsBrokerError};

/// One bounded response normalized before it can be bound to the operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ValidatedAlterShareGroupOffsetsResponse {
    /// Exact top-level broker rejection and its retained byte charge.
    BrokerRejected {
        error: AlterShareGroupOffsetsBrokerError,
        retained_bytes: usize,
    },
    /// Exact caller-ordered partition outcomes and their retained byte charge.
    Batch {
        batch: AlterShareGroupOffsetsBatch,
        retained_bytes: usize,
    },
}

impl ValidatedAlterShareGroupOffsetsResponse {
    pub(crate) fn into_parts(
        self,
    ) -> (
        Result<AlterShareGroupOffsetsBatch, AlterShareGroupOffsetsBrokerError>,
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
