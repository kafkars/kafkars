//! Exact private transaction request recovery at the engine boundary.

use std::time::Duration;

use crate::{
    Engine, EngineConfig,
    transaction::{TransactionInitializationAdmissionErrorKind, TransactionInitializationRequest},
};

#[test]
fn local_rejection_returns_the_exact_transaction_request() {
    let engine = Engine::start(EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("start engine: {error}"));
    let request = TransactionInitializationRequest::new(String::new(), 45_000);
    let capture = engine
        .capture_transactional_owner_initialization(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture transaction deadline: {error}"));
    let error = capture
        .initialize_transactional_owner(request)
        .err()
        .unwrap_or_else(|| panic!("empty transactional id must reject locally"));
    assert_eq!(
        error.kind(),
        TransactionInitializationAdmissionErrorKind::InvalidRequest
    );
    assert_eq!(error.into_request().into_parts(), (String::new(), 45_000));
    engine
        .shutdown()
        .unwrap_or_else(|error| panic!("shutdown engine: {error}"));
}

#[test]
fn accepted_initialization_returns_one_linear_observer() {
    let engine = Engine::start(EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("start engine: {error}"));
    let request = TransactionInitializationRequest::new("writer".to_owned(), 45_000);
    let capture = engine
        .capture_transactional_owner_initialization(Duration::from_millis(1))
        .unwrap_or_else(|error| panic!("capture transaction deadline: {error}"));
    let accepted = capture
        .initialize_transactional_owner(request)
        .unwrap_or_else(|error| panic!("accept transaction initialization: {error:?}"));

    assert!(accepted.fault().is_none());
    drop(accepted.into_observer());

    engine
        .shutdown()
        .unwrap_or_else(|error| panic!("shutdown engine: {error}"));
}
