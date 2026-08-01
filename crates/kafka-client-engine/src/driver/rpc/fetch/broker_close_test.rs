//! Live final-epoch Fetch-session close request execution.

use std::time::{Duration, Instant};

use kafka_client_core::Deadline;

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::DriverOwner,
    protocol::fetch::{FetchRequestSettings, FetchSessionRequest},
};

use super::{
    broker_close::{BrokerFetchCloseCall, BrokerFetchCloseSubmitError},
    route::BrokerId,
    submission::FetchSubmitError,
};

#[test]
fn established_session_close_fails_closed_without_exact_broker_routing() {
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build close driver: {error}"));
    let session =
        FetchSessionRequest::incremental(91, 7).unwrap_or_else(|| panic!("established session"));
    let error = BrokerFetchCloseCall::submit(
        &driver,
        BrokerId::new(1).unwrap_or_else(|error| panic!("broker ID: {error}")),
        FetchRequestSettings::new(500, 1, 1024, 1024, 0),
        session,
        OperationDeadline::from_parts_for_test(
            Deadline::from_tick(60_000_000_000),
            Instant::now() + Duration::from_secs(60),
        ),
    )
    .err()
    .unwrap_or_else(|| panic!("exact-broker close must remain unsent"));

    assert!(!error.is_backpressured());
    assert!(matches!(
        error,
        BrokerFetchCloseSubmitError::Driver(FetchSubmitError::ExactBrokerRoutingUnavailable)
    ));
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown close driver: {error}"));
}
