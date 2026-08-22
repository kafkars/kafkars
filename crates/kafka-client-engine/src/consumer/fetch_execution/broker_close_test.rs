//! Empty and locally removable broker-session close scheduling evidence.

use std::sync::Arc;

use kafka_client_core::Moment;

use crate::{
    clock::MonotonicClock,
    driver::BrokerId,
    protocol::fetch::{FetchSessionRequest, FetchSessionUpdate},
};

use super::{
    admission_test::{configure_broker_sessions, owner, shutdown},
    broker_session::BrokerSessionMember,
    executor::DirectFetchExecutor,
    settlement_test::{assignment, fetch_fence},
};

#[test]
fn requested_close_without_retained_sessions_clears_and_leaves_local_close_runnable() {
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, 1_024);
    executor
        .try_enable_sessions(1)
        .unwrap_or_else(|()| panic!("reserve broker-session capacity"));
    executor.request_broker_session_close();
    assert!(executor.broker_session_close_requested());
    assert_eq!(executor.retained_broker_sessions(), 0);

    let mut driver = owner();
    assert!(
        !executor
            .drive_broker_session_close(&driver, &MonotonicClock::new(), Moment::from_tick(0))
            .unwrap_or_else(|error| panic!("drive empty broker-session close: {error:?}"))
    );
    assert!(!executor.broker_session_close_requested());
    assert_eq!(executor.broker_session_close_deadline(), None);
    shutdown(&mut driver);
}

#[test]
fn initial_session_is_removed_locally_and_reports_progress() {
    let (effect, _machine) = assignment();
    let broker = BrokerId::new(3).unwrap_or_else(|error| panic!("broker ID: {error}"));
    let member =
        BrokerSessionMember::new(fetch_fence(effect).position(), Arc::from("events"), [7; 16]);
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, 1_024);
    executor
        .try_enable_sessions(1)
        .unwrap_or_else(|()| panic!("reserve broker-session capacity"));
    configure_broker_sessions(&mut executor);
    let initial = executor
        .broker_sessions
        .as_mut()
        .unwrap_or_else(|| panic!("broker sessions"))
        .try_begin(broker, vec![member])
        .unwrap_or_else(|(error, _active)| panic!("initial plan: {error:?}"));
    executor
        .broker_sessions
        .as_mut()
        .unwrap_or_else(|| panic!("broker sessions"))
        .complete(
            initial,
            FetchSessionUpdate::Continue(FetchSessionRequest::INITIAL),
        )
        .unwrap_or_else(|error| panic!("complete initial session: {error:?}"));
    executor.request_broker_session_close();

    let mut driver = owner();
    assert!(
        executor
            .drive_broker_session_close(&driver, &MonotonicClock::new(), Moment::from_tick(0))
            .unwrap_or_else(|error| panic!("drive local broker-session close: {error:?}"))
    );
    assert!(!executor.broker_session_close_requested());
    assert_eq!(executor.broker_session_close_deadline(), None);
    assert_eq!(executor.retained_broker_sessions(), 0);
    shutdown(&mut driver);
}
