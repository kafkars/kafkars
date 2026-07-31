//! Live final-epoch Fetch-session close request execution.

use std::time::{Duration, Instant};

use kafka_client_core::Deadline;
use kafka_driver::BrokerId;

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::DriverOwner,
    protocol::fetch::{FetchRequestSettings, FetchSessionRequest},
};

use super::{
    broker_close::BrokerFetchCloseCall,
    routed_response_broker_test::{self as broker, RoutedBroker},
};

#[test]
fn established_session_close_reaches_its_exact_broker_as_final_epoch() {
    let mut broker = RoutedBroker::new();
    let mut driver = DriverOwner::build(&EngineConfig::new(vec![broker.endpoint()]))
        .unwrap_or_else(|error| panic!("build close driver: {error}"));
    RoutedBroker::await_seed(&mut driver);
    broker.install_cluster(&mut driver);
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
    .unwrap_or_else(|error| panic!("submit close: {error:?}"));

    let (_version, request) = broker.complete_fetch_request(&mut driver);
    assert_eq!((request.session_id, request.session_epoch), (91, -1));
    assert!(request.topics.is_empty());
    assert!(request.forgotten_topics_data.is_empty());
    for _turn in 0..32 {
        if call
            .poll()
            .unwrap_or_else(|error| panic!("poll close: {error}"))
        {
            driver
                .shutdown_with_turn_limit(64, Duration::from_millis(10))
                .unwrap_or_else(|error| panic!("shutdown close driver: {error}"));
            return;
        }
        broker::drive(
            &mut driver,
            Duration::from_millis(100),
            "settle session close",
        );
    }
    panic!("session close did not settle")
}
