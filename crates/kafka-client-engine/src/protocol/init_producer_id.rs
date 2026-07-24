//! Generated nontransactional producer-identity request and response boundary.

use core::num::NonZeroI16;

use kafka_wire::{InitProducerIdRequest, InitProducerIdResponse};

const NO_PRODUCER_ID: i64 = -1;
const NO_PRODUCER_EPOCH: i16 = -1;

/// Valid producer identity returned by Kafka for idempotent record batches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InitializedProducerIdentity {
    producer_id: i64,
    producer_epoch: i16,
}

impl InitializedProducerIdentity {
    /// Returns the nonnegative broker-assigned producer ID.
    pub(crate) const fn producer_id(self) -> i64 {
        self.producer_id
    }

    /// Returns the nonnegative broker-assigned producer epoch.
    pub(crate) const fn producer_epoch(self) -> i16 {
        self.producer_epoch
    }
}

/// Semantic failure before a generated identity response reaches core policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InitProducerIdResponseFailure {
    /// Kafka returned a nonzero signed broker error code.
    Broker {
        /// Exact broker code, including unknown future values.
        code: NonZeroI16,
    },
    /// A successful response carried the protocol sentinel or an invalid ID.
    InvalidProducerId {
        /// Exact invalid value returned by Kafka.
        actual: i64,
    },
    /// A successful response carried the protocol sentinel or an invalid epoch.
    InvalidProducerEpoch {
        /// Exact invalid value returned by Kafka.
        actual: i16,
    },
}

/// Builds the generated request for a new nontransactional producer identity.
pub(crate) fn nontransactional_init_producer_id_request() -> InitProducerIdRequest {
    let mut request = InitProducerIdRequest::default();
    request.transactional_id = None;
    request.transaction_timeout_ms = i32::MAX;
    request.producer_id = NO_PRODUCER_ID;
    request.producer_epoch = NO_PRODUCER_EPOCH;
    request.enable2_pc = false;
    request.keep_prepared_txn = false;
    request
}

/// Normalizes one generated response without choosing recovery or retry policy.
pub(crate) fn normalize_init_producer_id_response(
    response: &InitProducerIdResponse,
) -> Result<InitializedProducerIdentity, InitProducerIdResponseFailure> {
    if let Some(code) = NonZeroI16::new(response.error_code) {
        return Err(InitProducerIdResponseFailure::Broker { code });
    }
    if response.producer_id < 0 {
        return Err(InitProducerIdResponseFailure::InvalidProducerId {
            actual: response.producer_id,
        });
    }
    if response.producer_epoch < 0 {
        return Err(InitProducerIdResponseFailure::InvalidProducerEpoch {
            actual: response.producer_epoch,
        });
    }
    Ok(InitializedProducerIdentity {
        producer_id: response.producer_id,
        producer_epoch: response.producer_epoch,
    })
}
