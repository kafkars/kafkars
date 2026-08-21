//! Live final-epoch Fetch-session close request execution.

use std::time::{Duration, Instant};

use kafka_client_core::Deadline;

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::DriverOwner,
    protocol::fetch::{FetchRequestSettings, FetchSessionRequest},
};

use super::{broker_close::BrokerFetchCloseCall, route::BrokerId};

#[test]
fn established_session_close_is_admitted_to_exact_broker_route() {
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build close driver: {error}"));
    let session =
        FetchSessionRequest::incremental(91, 7).unwrap_or_else(|| panic!("established session"));
    let mut call = BrokerFetchCloseCall::submit(
        &driver,
        BrokerId::new(1).unwrap_or_else(|error| panic!("broker ID: {error}")),
        FetchRequestSettings::new(500, 1, 1024, 1024, 0),
        session,
        OperationDeadline::from_parts_for_test(
            Deadline::from_tick(60_000_000_000),
            Instant::now() + Duration::from_secs(60),
        ),
    )
    .unwrap_or_else(|error| panic!("exact-broker close admission: {error:?}"));

    assert!(
        !call
            .poll()
            .unwrap_or_else(|error| panic!("close poll: {error}"))
    );
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown close driver: {error}"));
    drop(call);
}
