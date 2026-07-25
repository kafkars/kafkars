//! Join handoff, driver-owned deadline, and shutdown-recovery scenarios.

use std::time::Duration;

use kafka_client_core::{ClassicGroupPhase, ClassicGroupTiming, ClassicProtocol, GroupId};

use crate::clock::{DeadlineCapture, MonotonicClock};

use super::{
    classic_group_execution::{ClassicGroupExecution, new_classic_group_execution},
    classic_group_join::ClassicGroupJoinTracking,
    classic_group_owner::ClassicGroupOwner,
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
    let acceptance = handoff.into_driver_acceptance();
    let tracking = execution
        .confirm_join_driver_owned(acceptance)
        .unwrap_or_else(|(error, _acceptance)| panic!("driver confirmation failed: {error:?}"));

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
    assert_eq!(tracking.identity().group_id(), owner.machine().group_id());
    assert_eq!(tracking.identity().protocol(), ClassicProtocol::Range);
    assert_eq!(tracking.identity().timing(), owner.machine().timing());
    assert_eq!(tracking.identity().deadline(), capture.operation_deadline());
    execution
        .recover_join_after_driver_shutdown(tracking)
        .unwrap_or_else(|(error, _tracking)| panic!("driver recovery failed: {error:?}"));
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
    let acceptance = handoff.into_driver_acceptance();
    let tracking = execution
        .confirm_join_driver_owned(acceptance)
        .unwrap_or_else(|(error, _acceptance)| panic!("driver confirmation failed: {error:?}"));

    execution
        .recover_join_after_driver_shutdown(tracking)
        .unwrap_or_else(|(error, _tracking)| panic!("driver recovery failed: {error:?}"));

    assert_eq!(execution.next_deadline(), Some(capture.deadline()));
    assert_eq!(execution.unsettled(), 1);
}

#[test]
fn cross_group_recovery_receipt_rejects_without_mutating_either_owner() {
    let capture = MonotonicClock::new()
        .capture_deadline_after(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("deadline capture failed: {error}"));
    let (mut first_owner, mut first_execution, first_tracking) = driver_owned(group_id(1), capture);
    let (mut second_owner, mut second_execution, second_tracking) =
        driver_owned(group_id(2), capture);

    let (error, second_tracking) = first_execution
        .recover_join_after_driver_shutdown(second_tracking)
        .err()
        .unwrap_or_else(|| panic!("cross-group recovery must reject"));

    assert_eq!(
        error,
        super::classic_group_execution::ClassicGroupExecutionError::HandoffMismatch
    );
    assert_eq!(first_execution.next_deadline(), None);
    assert_eq!(first_execution.unsettled(), 1);
    assert_eq!(second_tracking.identity().group_id(), group_id(2));
    first_execution
        .recover_join_after_driver_shutdown(first_tracking)
        .unwrap_or_else(|(error, _tracking)| panic!("first recovery failed: {error:?}"));
    second_execution
        .recover_join_after_driver_shutdown(second_tracking)
        .unwrap_or_else(|(error, _tracking)| panic!("second recovery failed: {error:?}"));
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
    let (mut first_owner, mut first_execution, first_tracking) =
        driver_owned(group_id(1), first_capture);
    let (mut second_owner, mut second_execution, second_tracking) =
        driver_owned(group_id(1), second_capture);

    let (error, second_tracking) = first_execution
        .recover_join_after_driver_shutdown(second_tracking)
        .err()
        .unwrap_or_else(|| panic!("changed-deadline recovery must reject"));

    assert_eq!(
        error,
        super::classic_group_execution::ClassicGroupExecutionError::HandoffMismatch
    );
    assert_eq!(first_execution.next_deadline(), None);
    assert_eq!(
        second_tracking.identity().deadline(),
        second_capture.operation_deadline()
    );
    first_execution
        .recover_join_after_driver_shutdown(first_tracking)
        .unwrap_or_else(|(error, _tracking)| panic!("first recovery failed: {error:?}"));
    second_execution
        .recover_join_after_driver_shutdown(second_tracking)
        .unwrap_or_else(|(error, _tracking)| panic!("second recovery failed: {error:?}"));
    close(&mut first_owner, &mut first_execution);
    close(&mut second_owner, &mut second_execution);
}

fn owner() -> ClassicGroupOwner {
    ClassicGroupOwner::new(group_id(1), timing())
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
    ClassicGroupJoinTracking,
) {
    let mut owner = ClassicGroupOwner::new(group_id, timing());
    let mut execution = new_classic_group_execution();
    execution
        .begin(&mut owner, capture)
        .unwrap_or_else(|error| panic!("begin failed: {error:?}"));
    let handoff = execution
        .begin_join_handoff()
        .unwrap_or_else(|error| panic!("handoff failed: {error:?}"));
    let tracking = execution
        .confirm_join_driver_owned(handoff.into_driver_acceptance())
        .unwrap_or_else(|(error, _acceptance)| panic!("driver confirmation failed: {error:?}"));
    (owner, execution, tracking)
}

fn timing() -> ClassicGroupTiming {
    ClassicGroupTiming::try_new(12_345, 54_321)
        .unwrap_or_else(|error| panic!("valid classic group timing: {error}"))
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
        .close_if_local(owner, &mut catalog)
        .unwrap_or_else(|error| panic!("close failed: {error:?}"));
}
