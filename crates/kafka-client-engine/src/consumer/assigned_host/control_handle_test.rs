//! Public pause, resume, and seek admission over one unique engine handle.

use std::{sync::Arc, time::Duration};

use super::{
    AssignedConsumerAcceptedFaultKind, AssignedConsumerAssignment, AssignedConsumerClaimSlot,
    AssignedConsumerControlErrorKind, AssignedConsumerHandle, AssignedConsumerPartition,
    AssignedConsumerPort, AssignedConsumerShardOwner, AssignedConsumerStartPosition,
    shard_test::{FailingWake, setup},
};
use crate::{clock::MonotonicClock, consumer::assigned_owner_effect::FrontEffect};

#[test]
fn pause_resume_and_seek_reuse_one_opaque_assignment_epoch() {
    let (owner, port, wake) = setup();
    let mut handle = claim(port);
    let epoch = assign(&mut handle, "orders", 0);
    drain_effects(&owner);

    let paused = handle
        .try_pause(epoch, target("orders", 0))
        .unwrap_or_else(|error| panic!("pause: {error}"));
    assert_eq!(paused.fault(), None);
    drain_effects(&owner);

    let resumed = handle
        .try_resume(epoch, target("orders", 0), Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("resume: {error}"));
    assert_eq!(resumed.fault(), None);
    drain_effects(&owner);

    let sought = handle
        .try_seek(
            epoch,
            target("orders", 0),
            AssignedConsumerStartPosition::End,
            Duration::from_secs(1),
        )
        .unwrap_or_else(|error| panic!("seek: {error}"));
    assert_eq!(sought.fault(), None);
    assert_eq!(wake.count(), 4);
}

#[test]
fn retained_topic_translation_preserves_core_epoch_and_partition_fencing() {
    let (owner, port, _wake) = setup();
    let mut handle = claim(port);
    let stale = assign(&mut handle, "orders", 0);
    drain_effects(&owner);
    let active = assign(&mut handle, "payments", 0);
    drain_effects(&owner);

    let stale_error = handle
        .try_pause(stale, target("payments", 0))
        .err()
        .unwrap_or_else(|| panic!("stale epoch must fail"));
    assert_eq!(
        stale_error.kind(),
        AssignedConsumerControlErrorKind::StaleAssignment
    );

    let unknown_error = handle
        .try_pause(active, target("orders", 0))
        .err()
        .unwrap_or_else(|| panic!("revoked partition must fail"));
    assert_eq!(
        unknown_error.kind(),
        AssignedConsumerControlErrorKind::UnknownPartition
    );
}

#[test]
fn resume_and_seek_capture_deadlines_before_owner_contention() {
    let (owner, port, _wake) = setup();
    let mut handle = claim(port);
    let epoch = assign(&mut handle, "orders", 0);
    drain_effects(&owner);
    let guard = owner.lock_for_test();

    let resume = handle
        .try_resume(epoch, target("orders", 0), Duration::MAX)
        .err()
        .unwrap_or_else(|| panic!("resume deadline must fail first"));
    let seek = handle
        .try_seek(
            epoch,
            target("orders", 0),
            AssignedConsumerStartPosition::Beginning,
            Duration::MAX,
        )
        .err()
        .unwrap_or_else(|| panic!("seek deadline must fail first"));
    drop(guard);

    assert_eq!(
        resume.kind(),
        AssignedConsumerControlErrorKind::DeadlineOverflow
    );
    assert_eq!(
        seek.kind(),
        AssignedConsumerControlErrorKind::DeadlineOverflow
    );
}

#[test]
fn invalid_seek_offset_is_rejected_after_call_boundary_capture() {
    let (owner, port, _wake) = setup();
    let mut handle = claim(port);
    let epoch = assign(&mut handle, "orders", 0);
    drain_effects(&owner);

    let error = handle
        .try_seek(
            epoch,
            target("orders", 0),
            AssignedConsumerStartPosition::Offset(-1),
            Duration::from_secs(1),
        )
        .err()
        .unwrap_or_else(|| panic!("negative seek must fail"));

    assert_eq!(
        error.kind(),
        AssignedConsumerControlErrorKind::NegativeOffset
    );
}

#[test]
fn accepted_pause_retains_post_commit_wake_failure() {
    let clock = Arc::new(MonotonicClock::new());
    let wake = Arc::new(FailingWake);
    let (owner, port) = AssignedConsumerShardOwner::new_for_test(
        clock,
        super::super::assigned_owner_test::settings(),
        super::super::assigned_owner_test::limits(2),
        wake,
    )
    .unwrap_or_else(|error| panic!("assigned shard: {error:?}"));
    let mut handle = claim(port);
    let epoch = assign(&mut handle, "orders", 0);
    drain_effects(&owner);

    let accepted = handle
        .try_pause(epoch, target("orders", 0))
        .unwrap_or_else(|error| panic!("wake failure cannot revoke pause: {error}"));

    assert_eq!(
        accepted.fault(),
        Some(AssignedConsumerAcceptedFaultKind::Wake)
    );
}

fn claim(port: AssignedConsumerPort) -> AssignedConsumerHandle {
    let (slot, _closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    slot.claim(lifetime)
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"))
}

fn assign(
    handle: &mut AssignedConsumerHandle,
    topic: &str,
    partition: i32,
) -> super::AssignedConsumerAssignmentEpoch {
    handle
        .try_replace_assignment(
            vec![
                AssignedConsumerAssignment::try_new(
                    topic,
                    partition,
                    AssignedConsumerStartPosition::Offset(4),
                )
                .unwrap_or_else(|error| panic!("assignment: {error}")),
            ],
            Duration::from_secs(1),
        )
        .unwrap_or_else(|error| panic!("assignment admission: {error}"))
        .epoch()
}

fn target(topic: &str, partition: i32) -> AssignedConsumerPartition {
    AssignedConsumerPartition::try_new(topic, partition)
        .unwrap_or_else(|error| panic!("control target: {error}"))
}

fn drain_effects(owner: &AssignedConsumerShardOwner) {
    owner
        .try_with_owner(|assigned| {
            while !assigned.effects.is_empty() {
                assert_eq!(assigned.interpret_front_effect(), FrontEffect::Interpreted);
            }
        })
        .unwrap_or_else(|error| panic!("drain assigned effects: {error:?}"));
}
