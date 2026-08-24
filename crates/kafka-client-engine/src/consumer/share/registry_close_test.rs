//! Share close admission, leave, publication, control, and recovery scenarios.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{
    Moment, ShareGroupHeartbeatFailure, ShareGroupHeartbeatPhase, ShareGroupHeartbeatRequestKind,
};

use crate::{
    clock::MonotonicClock, driver::TopicPartitionCountFact,
    protocol::consumer::share_group::share_group_heartbeat_success_for_test,
};

use super::{
    close_state::ShareConsumerCloseTerminal,
    registry::ShareConsumerRegistry,
    registry_close::{ShareConsumerCloseAdmissionError, ShareConsumerCloseTurn},
    registry_topic_identity::complete_topic_identity,
};

#[test]
fn explicit_dormant_close_reserves_publishes_and_removes_exact_entry() {
    let (mut registry, group_id, clock) = registered();
    let capture = capture(&clock);
    let observer = registry
        .begin_explicit_close(group_id, capture)
        .unwrap_or_else(|error| panic!("close: {error:?}"));

    assert_eq!(
        turn(&mut registry, capture.now()),
        ShareConsumerCloseTurn::Progress
    );
    assert_eq!(
        turn(&mut registry, capture.now()),
        ShareConsumerCloseTurn::Progress
    );
    assert_eq!(observer.wait(), Ok(ShareConsumerCloseTerminal::Succeeded));
    assert!(registry.entry(group_id).is_none());
    assert_eq!(registry.retained_name_bytes(), 0);
}

#[test]
fn stable_member_prepares_epoch_minus_one_leave_before_success() {
    let (mut registry, group_id, clock) = membership();
    let capture = capture(&clock);
    let observer = registry
        .begin_explicit_close(group_id, capture)
        .unwrap_or_else(|error| panic!("close: {error:?}"));

    assert_eq!(
        turn(&mut registry, capture.now()),
        ShareConsumerCloseTurn::Blocked
    );
    let membership = registry
        .entry_mut(group_id)
        .and_then(|entry| entry.membership.as_mut())
        .unwrap_or_else(|| panic!("membership"));
    assert_eq!(
        membership.machine().phase(),
        ShareGroupHeartbeatPhase::Leaving
    );
    assert_eq!(
        membership.prepared().map(|prepared| prepared.kind),
        Some(ShareGroupHeartbeatRequestKind::Leave)
    );
    membership
        .settle_leave_success()
        .unwrap_or_else(|error| panic!("leave: {error:?}"));
    assert_eq!(
        turn(&mut registry, capture.now()),
        ShareConsumerCloseTurn::Progress
    );
    assert_eq!(
        turn(&mut registry, capture.now()),
        ShareConsumerCloseTurn::Progress
    );
    assert_eq!(observer.wait(), Ok(ShareConsumerCloseTerminal::Succeeded));
}

#[test]
fn elapsed_close_retains_exact_deadline_failure() {
    let (mut registry, group_id, clock) = membership();
    let capture = capture(&clock);
    let observer = registry
        .begin_explicit_close(group_id, capture)
        .unwrap_or_else(|error| panic!("close: {error:?}"));
    let elapsed = Moment::from_tick(capture.deadline().tick());

    assert_eq!(
        turn(&mut registry, elapsed),
        ShareConsumerCloseTurn::Progress
    );
    assert_eq!(
        turn(&mut registry, elapsed),
        ShareConsumerCloseTurn::Progress
    );
    assert_eq!(
        observer.wait(),
        Ok(ShareConsumerCloseTerminal::Failed(
            ShareGroupHeartbeatFailure::DeadlineElapsed
        ))
    );
}

#[test]
fn close_supersedes_a_prepared_steady_rediscovery_retry() {
    let (mut registry, group_id, clock) = membership();
    let entry = registry
        .entry_mut(group_id)
        .unwrap_or_else(|| panic!("entry"));
    let membership = entry
        .membership
        .as_mut()
        .unwrap_or_else(|| panic!("membership"));
    let schedule = membership
        .machine()
        .schedule()
        .unwrap_or_else(|| panic!("schedule"));
    membership
        .prepare_heartbeat_due(Moment::from_tick(schedule.deadline().tick()), &clock)
        .unwrap_or_else(|error| panic!("prepare steady: {error:?}"));
    let rejected = membership
        .prepared()
        .unwrap_or_else(|| panic!("prepared steady"))
        .attempt;
    let _retry = membership
        .settle_failure(
            Moment::from_tick(schedule.deadline().tick().saturating_add(1)),
            &clock,
            ShareGroupHeartbeatFailure::CoordinatorUnavailable,
        )
        .unwrap_or_else(|error| panic!("schedule rediscovery: {error:?}"));
    let retry = membership
        .prepared()
        .unwrap_or_else(|| panic!("prepared retry"))
        .attempt;
    assert_ne!(retry, rejected);
    assert!(membership.machine().retry_schedule().is_some());
    let capture = capture(&clock);
    let _observer = registry
        .begin_explicit_close(group_id, capture)
        .unwrap_or_else(|error| panic!("close: {error:?}"));

    assert_eq!(
        turn(&mut registry, capture.now()),
        ShareConsumerCloseTurn::Blocked
    );
    let membership = registry
        .entry(group_id)
        .and_then(|entry| entry.membership.as_ref())
        .unwrap_or_else(|| panic!("membership after close"));
    let leave = membership
        .prepared()
        .unwrap_or_else(|| panic!("prepared leave"));
    assert_eq!(leave.kind, ShareGroupHeartbeatRequestKind::Leave);
    assert_ne!(leave.attempt, retry);
    assert_eq!(
        membership.machine().phase(),
        ShareGroupHeartbeatPhase::Leaving
    );
}

#[test]
fn control_close_needs_no_completion_and_recovery_settles_explicit_observer() {
    let (mut registry, first, clock) = registered();
    let second = registry
        .try_register(
            Arc::from("workers-2"),
            None,
            vec![Arc::from("jobs")],
            crate::EngineShareConsumerFetchConfig::default(),
        )
        .unwrap_or_else(|_error| panic!("second register"));
    let observer = registry
        .begin_explicit_close(first, capture(&clock))
        .unwrap_or_else(|error| panic!("close: {error:?}"));
    registry.request_control_close(capture(&clock));
    assert!(matches!(
        registry.begin_explicit_close(second, capture(&clock)),
        Err(ShareConsumerCloseAdmissionError::AlreadyClosing)
    ));

    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recovery: {error:?}"));
    assert_eq!(registry.registered_count(), 0);
    assert_eq!(
        observer.wait(),
        Ok(ShareConsumerCloseTerminal::Failed(
            ShareGroupHeartbeatFailure::Execution
        ))
    );
}

fn turn(registry: &mut ShareConsumerRegistry, now: Moment) -> ShareConsumerCloseTurn {
    registry
        .turn_one_close(now)
        .unwrap_or_else(|error| panic!("turn: {error:?}"))
}

fn registered() -> (
    ShareConsumerRegistry,
    kafka_client_core::GroupId,
    MonotonicClock,
) {
    let clock = MonotonicClock::new();
    let mut registry =
        ShareConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error}"));
    let group_id = registry
        .try_register(
            Arc::from("workers"),
            None,
            vec![Arc::from("jobs")],
            crate::EngineShareConsumerFetchConfig::default(),
        )
        .unwrap_or_else(|_error| panic!("register"));
    (registry, group_id, clock)
}

fn membership() -> (
    ShareConsumerRegistry,
    kafka_client_core::GroupId,
    MonotonicClock,
) {
    let (mut registry, group_id, clock) = registered();
    let start = capture(&clock);
    registry
        .try_begin(group_id, start)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    let entry = registry
        .entry_mut(group_id)
        .unwrap_or_else(|| panic!("entry"));
    let local_topic_id = entry.local_topic_id(0).unwrap_or_else(|| panic!("topic"));
    complete_topic_identity(
        entry,
        local_topic_id,
        Arc::from("jobs"),
        start.operation_deadline(),
        TopicPartitionCountFact {
            metadata_generation: 1,
            logical_partition_count: 1,
            kafka_topic_id: Some([7; 16]),
        },
    )
    .unwrap_or_else(|error| panic!("identity: {error:?}"));
    let member = Arc::clone(entry.member());
    entry
        .membership
        .as_mut()
        .unwrap_or_else(|| panic!("membership"))
        .settle_success(
            start.now(),
            share_group_heartbeat_success_for_test(
                Some(&member),
                1,
                5_000,
                vec![([7; 16], vec![0])],
            ),
        )
        .unwrap_or_else(|error| panic!("success: {error:?}"));
    (registry, group_id, clock)
}

fn capture(clock: &MonotonicClock) -> crate::clock::DeadlineCapture {
    clock
        .capture_deadline_after(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("capture: {error:?}"))
}
