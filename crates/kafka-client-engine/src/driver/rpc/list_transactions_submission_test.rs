//! Admin `ListTransactions` discovery, exact-broker, deadline, and version scenarios.

use std::time::{Duration, Instant};

use kafka_driver::{ApiVersion, BrokerId, Route, TrafficClass};

use super::list_transactions_submission::{
    ListTransactionsSubmitError, list_transactions_broker_options, list_transactions_broker_route,
    list_transactions_discovery_options, list_transactions_discovery_route,
};

#[test]
fn discovery_is_any_broker_routed_and_broker_routes_are_exact() {
    assert_eq!(list_transactions_discovery_route(), Route::AnyBroker);
    assert_eq!(
        list_transactions_broker_route(7),
        Ok(Route::Broker {
            broker_id: BrokerId::new(7).unwrap_or_else(|error| panic!("broker: {error}")),
        })
    );
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
