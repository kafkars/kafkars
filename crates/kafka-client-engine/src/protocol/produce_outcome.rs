//! Concrete Produce outcomes translated into deterministic core inputs.

use kafka_client_core::{
    BatchExecutionId, DeliveryStatus, Moment, ProducerAttemptFailureKind, ProducerInput,
};
use kafka_wire::ProduceResponse;

use super::produce_response::{ProduceResponseFailure, normalize_explicit_produce_response};

/// Converts one correlated explicit-partition response into a core fact.
///
/// Structural mismatches remain protocol-owned failures. Broker-declared
/// failures have already crossed driver ownership and preserve the response
/// normalizer's conservative delivery certainty.
pub(crate) fn explicit_produce_response_input(
    execution: BatchExecutionId,
    now: Moment,
    expected_topic: &str,
    expected_partition: i32,
    response: &ProduceResponse,
) -> Result<ProducerInput, ProduceResponseFailure> {
    match normalize_explicit_produce_response(response, expected_topic, expected_partition) {
        Ok(success) => Ok(ProducerInput::BrokerSucceeded { execution, success }),
        Err(ProduceResponseFailure::Broker { failure, delivery }) => {
            Ok(ProducerInput::BrokerFailed {
                execution,
                now,
                failure,
                delivery,
                route_refreshed: false,
            })
        }
        Err(failure @ ProduceResponseFailure::Protocol { .. }) => Err(failure),
    }
}

/// Joins an adapter-normalized driver certainty with its correlated batch.
pub(crate) const fn produce_transport_failure_input(
    execution: BatchExecutionId,
    now: Moment,
    failure: ProducerAttemptFailureKind,
    delivery: DeliveryStatus,
) -> ProducerInput {
    ProducerInput::TransportFailed {
        execution,
        now,
        failure,
        delivery,
        route_refreshed: false,
    }
}
