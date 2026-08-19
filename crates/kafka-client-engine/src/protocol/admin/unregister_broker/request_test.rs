//! Broker identity and empty flexible-extension request evidence.

use super::{UnregisterBrokerRequestFailure, unregister_broker_request};

#[test]
fn request_preserves_nonnegative_broker_identity() {
    let request =
        unregister_broker_request(17).unwrap_or_else(|error| panic!("valid broker: {error:?}"));

    assert_eq!(request.broker_id, 17);
    assert!(request.unknown_tagged_fields.is_empty());
}

#[test]
fn request_rejects_negative_broker_identity() {
    assert_eq!(
        unregister_broker_request(-1),
        Err(UnregisterBrokerRequestFailure::NegativeBrokerId { actual: -1 })
    );
}
