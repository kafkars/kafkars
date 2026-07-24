//! Deadline-first port admission and ownership-preserving wake scenarios.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{AssignedTopicPartition, PartitionIndex, StartPosition, TopicId};

use super::super::{
    assigned_owner_close_test::install_pending_ready,
    assigned_owner_effect::FrontEffect,
    assigned_owner_test::{input, limits, settings},
};
use super::{
    result::{AssignedConsumerAcceptedFaultKind, AssignedConsumerPortError},
    shard::AssignedConsumerShardOwner,
    shard_test::{FailingWake, setup},
};
use crate::clock::MonotonicClock;

#[test]
fn deadline_capture_precedes_closed_check() {
    let (owner, port, _wake) = setup();
    owner
        .close_assigned_admission()
        .unwrap_or_else(|error| panic!("close admission: {error:?}"));
    let error = port
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Beginning)],
            Duration::MAX,
        )
        .err()
        .unwrap_or_else(|| panic!("overflowing deadline must reject"));
    assert!(matches!(&error, AssignedConsumerPortError::Clock(_)));
    assert_eq!(
        error.clock_error(),
        Some(crate::clock::ClockError::InstantOverflow)
    );
}

#[test]
fn deadline_capture_precedes_contended_check() {
    let (owner, port, _wake) = setup();
    let guard = owner.lock_for_test();
    let error = port
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Beginning)],
            Duration::MAX,
        )
        .err()
        .unwrap_or_else(|| panic!("overflowing deadline must reject"));
    assert!(matches!(error, AssignedConsumerPortError::Clock(_)));
    drop(guard);
}

#[test]
fn wake_failure_is_advisory_after_assignment_commits() {
    let clock = Arc::new(MonotonicClock::new());
    let (owner, port) = AssignedConsumerShardOwner::new_for_test(
        clock,
        settings(),
        limits(1),
        Arc::new(FailingWake),
    )
    .unwrap_or_else(|error| panic!("assigned shard: {error:?}"));

    let accepted = port
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Beginning)],
            Duration::from_secs(1),
        )
        .unwrap_or_else(|error| panic!("wake failure cannot revoke assignment: {error:?}"));

    assert_eq!(
        accepted.fault(),
        Some(AssignedConsumerAcceptedFaultKind::Wake)
    );
    assert!(
        owner
            .try_with_owner(|assigned| assigned.unsettled())
            .is_ok()
    );
}

#[test]
fn pause_resume_and_seek_cross_the_same_synchronized_port() {
    let (owner, port, _wake) = setup();
    let epoch = port
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Offset(offset(4)))],
            Duration::from_secs(1),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"))
        .into_value();
    let partition = owner
        .try_with_owner(|assigned| {
            assert_eq!(assigned.interpret_front_effect(), FrontEffect::Interpreted);
            assigned.topics.partitions()[0].partition()
        })
        .unwrap_or_else(|error| panic!("assigned owner: {error:?}"));

    let _paused = port
        .pause(epoch, partition)
        .unwrap_or_else(|error| panic!("pause: {error:?}"));
    drain_effects(&owner);
    let _resumed = port
        .resume(epoch, partition, Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("resume: {error:?}"));
    drain_effects(&owner);
    let _sought = port
        .seek(
            epoch,
            partition,
            StartPosition::Beginning,
            Duration::from_secs(1),
        )
        .unwrap_or_else(|error| panic!("seek: {error:?}"));
    drain_effects(&owner);
}

#[test]
fn ordinary_owner_rejection_does_not_request_an_extra_wake() {
    let (_owner, port, wake) = setup();
    let epoch = port
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Beginning)],
            Duration::from_secs(1),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"))
        .into_value();

    let error = port
        .pause(
            epoch,
            AssignedTopicPartition::new(TopicId::from_raw(1), PartitionIndex::from_raw(0)),
        )
        .err()
        .unwrap_or_else(|| panic!("pending effects must reject control"));

    assert!(matches!(
        error,
        AssignedConsumerPortError::Owner {
            error: super::super::assigned_owner_model::AssignedConsumerOwnerError::EffectsPending,
            wake: None,
        }
    ));
    assert_eq!(wake.count(), 1);
}

#[test]
fn rejected_explicit_close_does_not_publish_the_closed_mirror() {
    let (owner, port, wake) = setup();
    let _accepted = port
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Beginning)],
            Duration::from_secs(1),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));
    let close = port
        .begin_close()
        .err()
        .unwrap_or_else(|| panic!("pending assignment effects must reject close"));
    assert!(matches!(
        close,
        AssignedConsumerPortError::Owner {
            error: super::super::assigned_owner_model::AssignedConsumerOwnerError::EffectsPending,
            wake: None,
        }
    ));
    owner
        .try_with_owner(|assigned| {
            assert_eq!(assigned.interpret_front_effect(), FrontEffect::Interpreted);
        })
        .unwrap_or_else(|error| panic!("drain assignment effect: {error:?}"));

    let accepted = port
        .begin_close()
        .unwrap_or_else(|error| panic!("mirror must remain open: {error:?}"));
    let _observer = accepted.into_value();
    assert_eq!(wake.count(), 2);
}

#[test]
fn closed_admission_wins_before_ready_delivery_extraction() {
    let (owner, port, _wake) = setup();
    let _accepted = port
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Offset(offset(10)))],
            Duration::from_secs(1),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));
    owner
        .try_with_owner(|assigned| {
            assert_eq!(assigned.interpret_front_effect(), FrontEffect::Interpreted);
            install_pending_ready(assigned, 10);
        })
        .unwrap_or_else(|error| panic!("prepare delivery: {error:?}"));

    owner
        .close_assigned_admission()
        .unwrap_or_else(|error| panic!("close admission: {error:?}"));

    assert!(matches!(
        port.take_delivery(),
        Err(AssignedConsumerPortError::Closed)
    ));
    owner
        .try_with_owner(|assigned| {
            let delivery = assigned
                .take_delivery()
                .unwrap_or_else(|error| panic!("owner delivery: {error:?}"))
                .unwrap_or_else(|| panic!("closed port must leave delivery retained"));
            drop(delivery);
        })
        .unwrap_or_else(|error| panic!("inspect owner: {error:?}"));
}

#[test]
fn transferred_reclaim_fault_still_requests_host_recovery() {
    let (owner, port, wake) = setup();
    let _accepted = port
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Offset(offset(10)))],
            Duration::from_secs(1),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));
    owner
        .try_with_owner(|assigned| {
            assert_eq!(assigned.interpret_front_effect(), FrontEffect::Interpreted);
            install_pending_ready(assigned, 10);
        })
        .unwrap_or_else(|error| panic!("prepare delivery: {error:?}"));
    let delivery = port
        .take_delivery()
        .unwrap_or_else(|error| panic!("take delivery: {error:?}"))
        .unwrap_or_else(|| panic!("ready delivery"));
    owner
        .try_with_owner(|assigned| assigned.fetches.install_fault_for_test())
        .unwrap_or_else(|error| panic!("install fetch fault: {error:?}"));

    let transferred = port
        .reclaim_delivery(delivery)
        .unwrap_or_else(|_rejection| panic!("lease reached the owner"));

    assert_eq!(
        transferred.into_value(),
        Err(super::super::assigned_owner_model::AssignedConsumerOwnerError::Faulted)
    );
    assert_eq!(wake.count(), 2);
}

fn offset(value: i64) -> kafka_client_core::NextFetchOffset {
    kafka_client_core::NextFetchOffset::try_from_raw(value)
        .unwrap_or_else(|| panic!("nonnegative offset"))
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
