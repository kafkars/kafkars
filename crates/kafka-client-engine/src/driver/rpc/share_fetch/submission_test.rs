//! Exact API-78 broker route, deadline, and long-poll lane evidence.

use std::time::{Duration, Instant};

use kafka_client_core::{Deadline, ShareFetchBrokerId};
use kafka_driver::{BrokerId, Route, SubmitError, TrafficClass};

use crate::clock::OperationDeadline;

use super::submission::{
    ShareFetchDriverSubmitError, ShareFetchDriverSubmitErrorKind, share_fetch_options,
    share_fetch_route,
};

#[test]
fn route_and_options_pin_one_broker_and_stable_v1() {
    let broker = ShareFetchBrokerId::try_from_raw(7).unwrap_or_else(|| panic!("valid broker"));
    assert_eq!(
        share_fetch_route(broker).unwrap_or_else(|error| panic!("valid route: {error}")),
        Route::Broker {
            broker_id: BrokerId::new(7).unwrap_or_else(|error| panic!("valid broker: {error}")),
        }
    );

    let transport = Instant::now() + Duration::from_secs(3);
    let options = share_fetch_options(OperationDeadline::from_parts_for_test(
        Deadline::from_tick(30),
        transport,
    ));
    assert_eq!(options.deadline(), transport);
    assert_eq!(options.traffic_class(), TrafficClass::LongPoll);
    assert_eq!(
        options
            .minimum_version()
            .map(kafka_driver::ApiVersion::value),
        Some(1)
    );
    assert_eq!(
        options
            .maximum_version()
            .map(kafka_driver::ApiVersion::value),
        Some(1)
    );
}

#[test]
fn only_bounded_driver_full_is_transient_submission_backpressure() {
    assert_eq!(
        ShareFetchDriverSubmitError::Driver(SubmitError::Full).kind(),
        ShareFetchDriverSubmitErrorKind::Full
    );
    assert_eq!(
        ShareFetchDriverSubmitError::Driver(SubmitError::Closed).kind(),
        ShareFetchDriverSubmitErrorKind::Terminal
    );
}
