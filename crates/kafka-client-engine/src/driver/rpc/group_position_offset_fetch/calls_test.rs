//! Capacity, duplicate-fence, acceptance, and driver rejection scenarios.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use kafka_client_core::{
    AssignmentGeneration, Deadline, GroupId, GroupPositionBootstrapInput, GroupPositionFence,
    MemberId, MembershipCycle,
};

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::DriverOwner,
    protocol::consumer::{
        GroupOffsetFetchPreparation, GroupOffsetFetchTopic, PreparedGroupOffsetFetchRequest,
        prepare_group_offset_fetch_request,
    },
};

use super::{
    admission::{GroupPositionOffsetFetchAdmission, GroupPositionOffsetFetchReturnReason},
    calls::TrackedGroupPositionOffsetFetchCalls,
    key::GroupPositionOffsetFetchKey,
};

#[test]
fn duplicate_precedes_capacity_and_returns_both_exact_pre_driver_owners() {
    let mut owner = owner();
    let mut calls = TrackedGroupPositionOffsetFetchCalls::new(1);
    calls.install_terminal_for_test(
        key(1, 100),
        Some(9),
        Ok(kafka_wire::OffsetFetchResponse::default()),
    );

    let GroupPositionOffsetFetchAdmission::Returned(returned) =
        calls.try_submit(&owner, key(1, 200), "readers", request())
    else {
        panic!("same fence must be returned as a duplicate");
    };
    let (returned_key, returned_request, reason) = returned.into_parts();
    assert_eq!(returned_key.fence(), fence(1));
    assert_eq!(
        returned_key.operation_deadline().core(),
        Deadline::from_tick(200)
    );
    assert!(returned_request.retained_bytes() > 0);
    assert_eq!(reason, GroupPositionOffsetFetchReturnReason::DuplicateFence);

    let GroupPositionOffsetFetchAdmission::Returned(returned) =
        calls.try_submit(&owner, key(2, 300), "readers", request())
    else {
        panic!("another fence must observe retained capacity");
    };
    let (returned_key, _request, reason) = returned.into_parts();
    assert_eq!(returned_key.fence(), fence(2));
    assert_eq!(
        reason,
        GroupPositionOffsetFetchReturnReason::Capacity { limit: 1 }
    );
    assert_eq!(calls.retained_group_position_offset_fetch_count(), 1);
    shutdown(&mut owner);
}

#[test]
fn accepted_receipt_matches_the_registry_owner_and_shutdown_recovers_the_key() {
    let mut owner = owner();
    let mut calls = TrackedGroupPositionOffsetFetchCalls::new(8);
    let GroupPositionOffsetFetchAdmission::Accepted(accepted) =
        calls.try_submit(&owner, key(3, 100), "readers", request())
    else {
        panic!("live driver must accept one position request");
    };
    assert_eq!(
        accepted.driver_accepted(),
        GroupPositionBootstrapInput::DriverAccepted { fence: fence(3) }
    );
    assert_eq!(calls.retained_group_position_offset_fetch_count(), 1);

    shutdown(&mut owner);
    let mut recovery = calls.recover_group_position_offset_fetches_after_driver_shutdown();
    let active = recovery
        .pop_active()
        .unwrap_or_else(|| panic!("active call recovered"));
    assert_eq!(active.fence(), accepted.fence());
    assert_eq!(active.operation_deadline().core(), Deadline::from_tick(100));
    assert!(recovery.pop_active().is_none());
    assert!(recovery.take_settled().is_none());
    assert_eq!(recovery.pending_fence(), None);
    assert!(recovery.take_completion().is_none());
    assert!(recovery.is_empty());
    accepted.confirm_receipt();
    assert_eq!(calls.retained_group_position_offset_fetch_count(), 0);
}

#[test]
fn closed_driver_rejection_returns_the_exact_key_without_retaining_a_call() {
    let mut owner = owner();
    shutdown(&mut owner);
    let mut calls = TrackedGroupPositionOffsetFetchCalls::new(8);
    let GroupPositionOffsetFetchAdmission::Rejected(failure) =
        calls.try_submit(&owner, key(4, 123), "readers", request())
    else {
        panic!("closed driver must reject definitely unsent");
    };
    let (returned_key, source) = failure.into_parts();
    assert_eq!(returned_key.fence(), fence(4));
    assert_eq!(
        returned_key.operation_deadline().core(),
        Deadline::from_tick(123)
    );
    assert!(matches!(
        source,
        super::submission::GroupPositionOffsetFetchSubmitError::Driver(
            kafka_driver::SubmitError::Closed
        )
    ));
    assert_eq!(calls.retained_group_position_offset_fetch_count(), 0);
}

pub(super) fn request() -> PreparedGroupOffsetFetchRequest {
    let preparation = prepare_group_offset_fetch_request(
        Arc::from("readers"),
        vec![GroupOffsetFetchTopic::new(Arc::from("events"), vec![0, 2])],
        usize::MAX,
    )
    .unwrap_or_else(|error| panic!("prepare request: {error:?}"));
    let GroupOffsetFetchPreparation::Prepared(prepared) = preparation else {
        panic!("nonempty assignment must prepare a request");
    };
    let (_correlation, request) = prepared.into_parts();
    request
}

pub(super) fn key(assignment_generation: u64, deadline: u64) -> GroupPositionOffsetFetchKey {
    GroupPositionOffsetFetchKey::new(
        fence(assignment_generation),
        OperationDeadline::from_parts_for_test(
            Deadline::from_tick(deadline),
            Instant::now() + Duration::from_secs(1),
        ),
    )
}

pub(super) fn fence(assignment_generation: u64) -> GroupPositionFence {
    GroupPositionFence::new(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group")),
        MembershipCycle::try_from_raw(2).unwrap_or_else(|| panic!("cycle")),
        MemberId::try_from_raw(3).unwrap_or_else(|| panic!("member")),
        AssignmentGeneration::try_from_raw(assignment_generation)
            .unwrap_or_else(|| panic!("assignment")),
    )
}

fn owner() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("build embedded driver owner: {error}"))
}

fn shutdown(owner: &mut DriverOwner) {
    owner
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("bounded driver shutdown: {error}"));
}
