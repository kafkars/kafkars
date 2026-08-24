//! Raw classic Heartbeat terminal retention scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{ClassicHeartbeatAttempt, Deadline, GroupId, MembershipCycle};
use kafka_driver::{ApiVersion, CallFailure, Delivery, RequestError, ResponseCloseReason};
use kafka_wire::HeartbeatResponse;

use crate::clock::OperationDeadline;

use super::{
    heartbeat_terminal::{
        ClassicHeartbeatCallKey, coordinator_path_lost, retain_classic_heartbeat_terminal,
    },
    heartbeat_test_fixture::heartbeat_attempts,
};

#[test]
fn only_route_loss_terminals_request_coordinator_recovery() {
    assert!(coordinator_path_lost(&RequestError::Rejected {
        failure: CallFailure::Closed,
        delivery: Delivery::NotSent,
    }));
    assert!(coordinator_path_lost(&RequestError::Rejected {
        failure: CallFailure::DeadlineExceeded,
        delivery: Delivery::NotSent,
    }));
    assert!(coordinator_path_lost(&RequestError::ConnectionClosed(
        ResponseCloseReason::TransportClosed,
    )));
    assert!(!coordinator_path_lost(&RequestError::Rejected {
        failure: CallFailure::CapacityReached { limit: 1 },
        delivery: Delivery::NotSent,
    }));
    assert!(!coordinator_path_lost(&RequestError::ConnectionClosed(
        ResponseCloseReason::Shutdown,
    )));
}

#[test]
fn raw_success_preserves_group_attempt_deadline_version_and_response() {
    let key = key(1);
    let mut response = HeartbeatResponse::default();
    response.error_code = -47;
    response.throttle_time_ms = 31;

    let terminal = retain_classic_heartbeat_terminal(key, Some(ApiVersion::new(2)), Ok(response));

    assert_eq!(terminal.key(), key);
    assert_eq!(terminal.key().group_id().get(), 1);
    assert_eq!(terminal.key().cycle(), MembershipCycle::initial());
    assert_eq!(terminal.key().attempt(), key.attempt());
    assert_eq!(terminal.key().deadline(), key.deadline());
    assert_eq!(terminal.selected_version(), Some(2));
    assert!(matches!(
        terminal.result(),
        Ok(response) if response.error_code == -47 && response.throttle_time_ms == 31
    ));
}

#[test]
fn raw_driver_failure_remains_uninterpreted() {
    let key = key(1);
    let terminal =
        retain_classic_heartbeat_terminal(key, None, Err(RequestError::RouteUnavailable));
    let (actual_key, version, result) = terminal.into_parts();

    assert_eq!(actual_key, key);
    assert_eq!(version, None);
    assert!(matches!(result, Err(RequestError::RouteUnavailable)));
}

pub(super) fn key(group: u64) -> ClassicHeartbeatCallKey {
    key_with_deadline(group, deadline())
}

pub(super) fn key_with_deadline(
    group: u64,
    deadline: OperationDeadline,
) -> ClassicHeartbeatCallKey {
    let (attempt, _next) = heartbeat_attempts();
    key_with_attempt_and_deadline(group, attempt, deadline)
}

pub(super) fn key_with_attempt_and_deadline(
    group: u64,
    attempt: ClassicHeartbeatAttempt,
    deadline: OperationDeadline,
) -> ClassicHeartbeatCallKey {
    let group_id = GroupId::try_from_raw(group)
        .unwrap_or_else(|| panic!("test group identity must be nonzero"));
    ClassicHeartbeatCallKey::new(group_id, attempt, deadline)
}

pub(super) fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        Deadline::from_tick(50),
        Instant::now() + Duration::from_secs(5),
    )
}
