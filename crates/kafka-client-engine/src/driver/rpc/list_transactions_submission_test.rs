//! Admin `ListTransactions` routing, options, and linear call-ownership scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::AdminListTransactionsPlan;
use kafka_driver::{ApiVersion, CompletionError, Route, TrafficClass};
use kafka_wire::ListTransactionsRequest;

use crate::{EngineConfig, driver::DriverOwner};

use super::{
    list_transactions_call::ListTransactionsCall,
    list_transactions_submission::{
        ListTransactionsSubmitError, list_transactions_broker_options,
        list_transactions_broker_route, list_transactions_discovery_options,
        list_transactions_discovery_route,
    },
};

#[test]
fn discovery_is_any_broker_routed_and_broker_routes_are_exact() {
    assert_eq!(list_transactions_discovery_route(), Route::AnyBroker);
    assert_eq!(list_transactions_broker_route(7), Ok(Route::AnyBroker));
    assert!(list_transactions_broker_route(-1).is_err());
}

#[test]
fn discovery_preserves_deadline_lane_and_describe_cluster_ceiling() {
    let deadline = Instant::now() + Duration::from_secs(9);
    let options = list_transactions_discovery_options(deadline);

    assert_eq!(options.deadline(), deadline);
    assert_eq!(options.traffic_class(), TrafficClass::Interactive);
    assert_eq!(options.maximum_version(), Some(ApiVersion::new(2)));
}

#[test]
fn broker_options_preserve_deadline_lane_and_explicit_floor() {
    let deadline = Instant::now() + Duration::from_secs(7);
    for minimum_version in 0..=2 {
        let options = list_transactions_broker_options(deadline, minimum_version)
            .unwrap_or_else(|error| panic!("valid options: {error}"));
        assert_eq!(options.deadline(), deadline);
        assert_eq!(options.traffic_class(), TrafficClass::Interactive);
        assert_eq!(
            options.minimum_version(),
            Some(ApiVersion::new(minimum_version))
        );
        assert_eq!(options.maximum_version(), Some(ApiVersion::new(2)));
    }
}

#[test]
fn broker_options_reject_out_of_range_version_floors() {
    let deadline = Instant::now() + Duration::from_secs(7);
    for actual in [-1, 3] {
        assert!(matches!(
            list_transactions_broker_options(deadline, actual),
            Err(ListTransactionsSubmitError::InvalidVersionFloor { actual: value })
                if value == actual
        ));
    }
}

#[test]
fn completion_fault_preserves_discovery_and_exact_broker_evidence() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let deadline = Instant::now() + Duration::from_secs(1);
    let retained_limit = 64 * 1024;
    let plan = plan();
    let mut discovery = ListTransactionsCall::submit_discovery(&driver, retained_limit, deadline)
        .unwrap_or_else(|_error| panic!("accepted discovery"));
    let mut broker = ListTransactionsCall::submit_broker(
        &driver,
        7,
        plan.clone(),
        retained_limit,
        ListTransactionsRequest::default(),
        0,
        deadline,
    )
    .unwrap_or_else(|_error| panic!("accepted broker call"));
    drop(driver);

    assert!(matches!(
        discovery.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    assert!(matches!(
        broker.try_terminal(),
        Some(Err(CompletionError::Closed))
    ));
    let discovery = discovery
        .recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("discovery correlation must survive"));
    let broker = broker
        .recover_after_driver_shutdown()
        .unwrap_or_else(|| panic!("broker correlation must survive"));
    assert!(discovery.matches_discovery(retained_limit));
    assert!(broker.matches_broker(7, &plan, retained_limit));
    discovery.seal_recovered();
    broker.seal_recovered();
}

#[test]
fn synchronous_rejection_returns_exact_broker_plan_and_limit() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let plan = plan();
    let retained_limit = 32 * 1024;
    let rejection = match ListTransactionsCall::submit_broker(
        &driver,
        -1,
        plan.clone(),
        retained_limit,
        ListTransactionsRequest::default(),
        0,
        Instant::now() + Duration::from_secs(1),
    ) {
        Ok(_call) => panic!("invalid broker must reject before driver ownership"),
        Err(rejection) => rejection,
    };

    assert_eq!(
        rejection.into_submission_evidence(),
        (Some((-1, plan)), retained_limit)
    );
}

fn plan() -> AdminListTransactionsPlan {
    AdminListTransactionsPlan::new(
        vec!["Ongoing".to_owned()],
        vec![-7],
        Some(42),
        Some("^orders".to_owned()),
    )
    .unwrap_or_else(|error| panic!("plan: {error}"))
}
