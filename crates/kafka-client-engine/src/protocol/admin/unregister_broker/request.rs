//! Explicit generated request construction for one nonnegative broker identity.

use kafka_wire::UnregisterBrokerRequest;

/// Invalid broker identity rejected before driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UnregisterBrokerRequestFailure {
    NegativeBrokerId { actual: i32 },
}

/// Builds the sole v0 request without inventing a Kafka-side timeout.
pub(crate) fn unregister_broker_request(
    broker_id: i32,
) -> Result<UnregisterBrokerRequest, UnregisterBrokerRequestFailure> {
    if broker_id < 0 {
        return Err(UnregisterBrokerRequestFailure::NegativeBrokerId { actual: broker_id });
    }
    let mut request = UnregisterBrokerRequest::default();
    request.broker_id = broker_id;
    Ok(request)
}
