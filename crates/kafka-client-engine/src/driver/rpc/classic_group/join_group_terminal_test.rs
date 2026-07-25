//! Raw Join terminal retention scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{Deadline, GroupId, MembershipCycle};
use kafka_driver::{ApiVersion, RequestError};
use kafka_wire::JoinGroupResponse;

use crate::clock::OperationDeadline;

use super::join_group_terminal::{JoinGroupCallKey, retain_join_group_terminal};

#[test]
fn raw_success_preserves_group_cycle_deadline_version_and_response() {
    let key = key(1);
    let mut response = JoinGroupResponse::default();
    response.error_code = -47;
    response.generation_id = 31;

    let terminal = retain_join_group_terminal(key, Some(ApiVersion::new(3)), Ok(response));

    assert_eq!(terminal.key(), key);
    assert_eq!(terminal.key().group_id().get(), 1);
    assert_eq!(terminal.key().cycle(), MembershipCycle::initial());
    assert_eq!(terminal.key().deadline(), key.deadline());
    assert_eq!(terminal.selected_version(), Some(3));
    assert!(matches!(
        terminal.result(),
        Ok(response) if response.error_code == -47 && response.generation_id == 31
    ));
}

#[test]
fn raw_driver_failure_remains_uninterpreted() {
    let key = key(1);
    let terminal = retain_join_group_terminal(key, None, Err(RequestError::RouteUnavailable));
    let (actual_key, version, result) = terminal.into_parts();

    assert_eq!(actual_key, key);
    assert_eq!(version, None);
    assert!(matches!(result, Err(RequestError::RouteUnavailable)));
}

pub(super) fn key(group: u64) -> JoinGroupCallKey {
    key_with_deadline(group, deadline())
}

pub(super) fn key_with_deadline(group: u64, deadline: OperationDeadline) -> JoinGroupCallKey {
    let group_id = GroupId::try_from_raw(group)
        .unwrap_or_else(|| panic!("test group identity must be nonzero"));
    JoinGroupCallKey::new(group_id, MembershipCycle::initial(), deadline)
}

pub(super) fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        Deadline::from_tick(50),
        Instant::now() + Duration::from_secs(5),
    )
}
