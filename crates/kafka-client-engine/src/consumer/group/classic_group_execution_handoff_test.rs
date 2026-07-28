//! Join handoff, driver-owned deadline, and shutdown-recovery scenarios.

use std::time::Duration;

use kafka_client_core::{
    ClassicGroupPhase, ClassicGroupTiming, ClassicHeartbeatPolicy, ClassicProcessingLease,
    ClassicProtocol, GroupId,
};

use crate::{
    clock::{DeadlineCapture, MonotonicClock},
    driver::classic_group::{AcceptedJoinGroupCall, JoinGroupCallKey, RecoveredJoinGroupOwnership},
};

use super::{
    classic_group_execution::{ClassicGroupExecution, new_classic_group_execution},
    classic_group_fetch::ClassicGroupFetchOwner,
    classic_group_owner::ClassicGroupOwner,
    classic_group_test_support,
    registry_entry::default_classic_processing_lease_policy,
};

#[test]
fn accepted_driver_ownership_disarms_local_deadline_expiration() {
    let mut owner = owner();
    let mut execution = new_classic_group_execution();
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_nanos(1))
        .unwrap_or_else(|error| panic!("deadline capture failed: {error}"));
    execution
        .begin(&mut owner, capture)
        .unwrap_or_else(|error| panic!("begin failed: {error:?}"));
    let handoff = execution
        .begin_join_handoff()
        .unwrap_or_else(|error| panic!("handoff failed: {error:?}"));
    let identity = handoff.identity();
    let acceptance = handoff.into_driver_acceptance();
    let key = JoinGroupCallKey::new(identity.group_id(), identity.cycle(), identity.deadline());
    execution
        .confirm_join_driver_owned(acceptance, AcceptedJoinGroupCall::from_key_for_test(key))
        .unwrap_or_else(|_failure| panic!("driver confirmation failed"));

    assert_eq!(execution.next_deadline(), None);
    assert_eq!(execution.unsettled(), 1);
    assert_eq!(
        execution.expire_if_due(
            &mut owner,
            kafka_client_core::Moment::from_tick(capture.deadline().tick()),
        ),
        Ok(false)
    );
    assert_eq!(owner.machine().phase(), ClassicGroupPhase::Joining);
    let retained = execution
        .join_call()
        .unwrap_or_else(|| panic!("accepted Join owner expected"));
    assert_eq!(retained.identity().group_id(), owner.machine().group_id());
    assert_eq!(retained.identity().protocol(), ClassicProtocol::Range);
    assert_eq!(retained.identity().timing(), owner.machine().timing());
    assert_eq!(retained.identity().deadline(), capture.operation_deadline());
    execution
        .reconcile_join_after_driver_shutdown(RecoveredJoinGroupOwnership::active_for_test(key))
        .unwrap_or_else(|(error, _recovered)| panic!("driver recovery failed: {error:?}"));
}

#[test]
fn rejected_driver_handoff_restores_the_exact_prepared_join() {
    let mut owner = owner();
    let mut execution = new_classic_group_execution();
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("deadline capture failed: {error}"));
    let cycle = execution
        .begin(&mut owner, capture)
        .unwrap_or_else(|error| panic!("begin failed: {error:?}"));
    let handoff = execution
        .begin_join_handoff()
        .unwrap_or_else(|error| panic!("handoff failed: {error:?}"));

    execution
        .restore_join(handoff)
        .unwrap_or_else(|(error, _handoff)| panic!("restore failed: {error:?}"));

    let restored = execution
        .prepared_join()
        .unwrap_or_else(|| panic!("restored Join expected"));
    assert_eq!(restored.cycle(), cycle);
    assert_eq!(restored.timing(), owner.machine().timing());
    assert_eq!(restored.deadline(), capture.operation_deadline());
}

#[test]
fn driver_shutdown_rearms_the_exact_original_join_deadline() {
    let mut owner = owner();
    let mut execution = new_classic_group_execution();
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("deadline capture failed: {error}"));
    execution
        .begin(&mut owner, capture)
        .unwrap_or_else(|error| panic!("begin failed: {error:?}"));
    let handoff = execution
        .begin_join_handoff()
        .unwrap_or_else(|error| panic!("handoff failed: {error:?}"));
    let identity = handoff.identity();
    let acceptance = handoff.into_driver_acceptance();
    let key = JoinGroupCallKey::new(identity.group_id(), identity.cycle(), identity.deadline());
    execution
        .confirm_join_driver_owned(acceptance, AcceptedJoinGroupCall::from_key_for_test(key))
        .unwrap_or_else(|_failure| panic!("driver confirmation failed"));

    execution
        .reconcile_join_after_driver_shutdown(RecoveredJoinGroupOwnership::active_for_test(key))
        .unwrap_or_else(|(error, _recovered)| panic!("driver recovery failed: {error:?}"));

    assert_eq!(execution.next_deadline(), Some(capture.deadline()));
    assert_eq!(execution.unsettled(), 1);
}

#[test]
fn cross_group_recovery_receipt_rejects_without_mutating_either_owner() {
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("deadline capture failed: {error}"));
    let (mut first_owner, mut first_execution, first_recovered) =
        driver_owned(group_id(1), capture);
    let (mut second_owner, mut second_execution, second_recovered) =
        driver_owned(group_id(2), capture);

    let (error, second_recovered) = first_execution
        .reconcile_join_after_driver_shutdown(second_recovered)
        .err()
        .unwrap_or_else(|| panic!("cross-group recovery must reject"));

    assert_eq!(
        error,
        super::classic_group_execution::ClassicGroupExecutionError::HandoffMismatch
    );
    assert_eq!(first_execution.next_deadline(), None);
    assert_eq!(first_execution.unsettled(), 1);
    assert_eq!(second_recovered.key().group_id(), group_id(2));
    first_execution
        .reconcile_join_after_driver_shutdown(first_recovered)
        .unwrap_or_else(|(error, _recovered)| panic!("first recovery failed: {error:?}"));
    second_execution
        .reconcile_join_after_driver_shutdown(second_recovered)
        .unwrap_or_else(|(error, _recovered)| panic!("second recovery failed: {error:?}"));
    close(&mut first_owner, &mut first_execution);
    close(&mut second_owner, &mut second_execution);
}

#[test]
fn changed_deadline_recovery_receipt_rejects_without_mutation() {
    let clock = MonotonicClock::new();
    let first_capture = clock
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("first deadline capture failed: {error}"));
    let second_capture = clock
        .capture_deadline_after(Duration::from_secs(2))
        .unwrap_or_else(|error| panic!("second deadline capture failed: {error}"));
    let (mut first_owner, mut first_execution, first_recovered) =
        driver_owned(group_id(1), first_capture);
    let (mut second_owner, mut second_execution, second_recovered) =
        driver_owned(group_id(1), second_capture);

    let (error, second_recovered) = first_execution
        .reconcile_join_after_driver_shutdown(second_recovered)
        .err()
        .unwrap_or_else(|| panic!("changed-deadline recovery must reject"));

    assert_eq!(
        error,
        super::classic_group_execution::ClassicGroupExecutionError::HandoffMismatch
    );
    assert_eq!(first_execution.next_deadline(), None);
    assert_eq!(
        second_recovered.key().deadline(),
        second_capture.operation_deadline()
    );
    first_execution
        .reconcile_join_after_driver_shutdown(first_recovered)
        .unwrap_or_else(|(error, _recovered)| panic!("first recovery failed: {error:?}"));
    second_execution
        .reconcile_join_after_driver_shutdown(second_recovered)
        .unwrap_or_else(|(error, _recovered)| panic!("second recovery failed: {error:?}"));
    close(&mut first_owner, &mut first_execution);
    close(&mut second_owner, &mut second_execution);
}

fn owner() -> ClassicGroupOwner {
    ClassicGroupOwner::new(
        group_id(1),
        timing(),
        heartbeat_policy(),
        classic_group_test_support::rejoin_policy(),
    )
}

fn group_id(raw: u64) -> GroupId {
    GroupId::try_from_raw(raw).unwrap_or_else(|| panic!("nonzero group identity"))
}

fn driver_owned(
    group_id: GroupId,
    capture: DeadlineCapture,
) -> (
    ClassicGroupOwner,
    ClassicGroupExecution,
    RecoveredJoinGroupOwnership,
) {
    let mut owner = ClassicGroupOwner::new(
        group_id,
        timing(),
        heartbeat_policy(),
        classic_group_test_support::rejoin_policy(),
    );
    let mut execution = new_classic_group_execution();
    execution
        .begin(&mut owner, capture)
        .unwrap_or_else(|error| panic!("begin failed: {error:?}"));
    let handoff = execution
        .begin_join_handoff()
        .unwrap_or_else(|error| panic!("handoff failed: {error:?}"));
    let identity = handoff.identity();
    let key = JoinGroupCallKey::new(identity.group_id(), identity.cycle(), identity.deadline());
    execution
        .confirm_join_driver_owned(
            handoff.into_driver_acceptance(),
            AcceptedJoinGroupCall::from_key_for_test(key),
        )
        .unwrap_or_else(|_failure| panic!("driver confirmation failed"));
    (
        owner,
        execution,
        RecoveredJoinGroupOwnership::active_for_test(key),
    )
}

fn timing() -> ClassicGroupTiming {
    ClassicGroupTiming::try_new(12_345, 54_321)
        .unwrap_or_else(|error| panic!("valid classic group timing: {error}"))
}

fn heartbeat_policy() -> ClassicHeartbeatPolicy {
    ClassicHeartbeatPolicy::try_new(1_000_000_000, 2_000_000_000)
        .unwrap_or_else(|error| panic!("valid heartbeat policy: {error}"))
}

fn close(owner: &mut ClassicGroupOwner, execution: &mut ClassicGroupExecution) {
    let group_id = owner.machine().group_id();
    let mut catalog = super::session_catalog::GroupSessionCatalog::try_new(
        group_id,
        std::sync::Arc::from("workers"),
        &[std::sync::Arc::from("orders")],
    )
    .unwrap_or_else(|error| panic!("catalog failed: {error:?}"));
    execution
        .close_if_local(
            owner,
            &mut catalog,
            &mut ClassicProcessingLease::new(default_classic_processing_lease_policy()),
            &mut ClassicGroupFetchOwner::try_new()
                .unwrap_or_else(|error| panic!("Fetch owner: {error:?}")),
        )
        .unwrap_or_else(|error| panic!("close failed: {error:?}"));
}
