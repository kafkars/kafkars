//! Engine-boundary validation retains caller input outside accepted ownership.

use super::{
    TransactionInitializationAdmissionErrorKind, TransactionInitializationRequest, port::validate,
};

#[test]
fn validation_rejects_invalid_identity_or_broker_timeout_without_mutation() {
    for request in [
        TransactionInitializationRequest::new(String::new(), 10),
        TransactionInitializationRequest::new("writer".to_owned(), 0),
        TransactionInitializationRequest::new("x".repeat(i16::MAX as usize + 1), 10),
    ] {
        let before = request.transactional_id().to_owned();
        assert_eq!(
            validate(&request),
            Err(TransactionInitializationAdmissionErrorKind::InvalidRequest)
        );
        assert_eq!(request.transactional_id(), before);
    }
}

#[test]
fn validation_rejects_hostile_string_capacity_without_losing_caller_ownership() {
    let mut transactional_id = String::with_capacity(i16::MAX as usize + 1);
    transactional_id.push_str("writer");
    let request = TransactionInitializationRequest::new(transactional_id, 10);
    assert_eq!(
        validate(&request),
        Err(TransactionInitializationAdmissionErrorKind::RetainedBytes)
    );
    let (transactional_id, timeout_ms) = request.into_parts();
    assert_eq!(transactional_id, "writer");
    assert!(transactional_id.capacity() > i16::MAX as usize);
    assert_eq!(timeout_ms, 10);
}
