//! Delivery fencing, lease reclamation, and close-quiescence scenarios.

use std::time::Duration;

use kafka_client_core::{AssignedConsumerMachine, NextFetchOffset, StartPosition};

use crate::completion::CompletionRegistryError;
use crate::protocol::fetch::fixture::encoded_data_batch_for_test;

use super::{
    assigned_owner::AssignedConsumerOwner,
    assigned_owner_effect::FrontEffect,
    assigned_owner_fault::{AssignedConsumerFaultKind, AssignedConsumerOwnerFault},
    assigned_owner_test::{input, owner},
    fetch_execution::{FetchTerminalFixture, install_terminal_for_test},
};

#[test]
fn unread_ready_delivery_is_reclaimed_during_close() {
    let mut owner = ready_owner();
    let observer = owner
        .begin_close()
        .unwrap_or_else(|error| panic!("begin close: {error:?}"));
    drain_effects(&mut owner);

    assert!(owner.progress_close());
    assert_eq!(owner.fetches.retained(), (0, 0, 0));
    assert!(owner.progress_close());
    assert_eq!(owner.interpret_front_effect(), FrontEffect::Interpreted);
    assert!(owner.progress_close());
    assert_eq!(observer.wait(), Ok(()));
}

#[test]
fn close_publication_backpressure_retains_the_exact_terminal_for_retry() {
    let mut owner = owner(1);
    let observer = owner
        .begin_close()
        .unwrap_or_else(|error| panic!("begin close: {error:?}"));
    assert_eq!(owner.interpret_front_effect(), FrontEffect::Interpreted);
    assert!(owner.progress_close());
    assert_eq!(owner.interpret_front_effect(), FrontEffect::Interpreted);
    owner.inject_close_publish_fault(CompletionRegistryError::NotificationBackpressure);

    assert!(!owner.progress_close());
    assert_eq!(
        owner.close.phase(),
        super::assigned_close_error::AssignedCloseSlotPhase::Ready
    );
    assert_eq!(owner.close_completions.unsettled_len(), 1);

    assert!(owner.progress_close());
    assert_eq!(observer.wait(), Ok(()));
}

#[test]
fn external_delivery_lease_delays_close_until_reclaimed() {
    let mut owner = ready_owner();
    let delivery = owner
        .take_delivery()
        .unwrap_or_else(|error| panic!("take delivery: {error:?}"))
        .unwrap_or_else(|| panic!("ready delivery"));
    let _observer = owner
        .begin_close()
        .unwrap_or_else(|error| panic!("begin close: {error:?}"));
    drain_effects(&mut owner);

    assert!(!owner.progress_close());
    owner
        .reclaim_delivery(delivery)
        .unwrap_or_else(|error| panic!("reclaim delivery: {error:?}"));
    assert!(owner.progress_close());
}

#[test]
fn stale_ready_assignment_is_reclaimed_instead_of_exposed() {
    let mut owner = ready_owner();
    owner
        .replace_assignment(
            vec![input("orders", 1, StartPosition::Offset(offset(20)))],
            Duration::from_secs(30),
        )
        .unwrap_or_else(|error| panic!("replace assignment: {error:?}"));
    drain_effects(&mut owner);

    assert!(
        owner
            .take_delivery()
            .unwrap_or_else(|error| panic!("discard stale delivery: {error:?}"))
            .is_none()
    );
    assert_eq!(owner.fetches.retained(), (0, 0, 0));
}

#[test]
fn stale_ready_entries_are_skipped_until_the_active_fifo_delivery() {
    let mut owner = ready_owner();
    let active_epoch = owner
        .replace_assignment(
            vec![input("orders", 1, StartPosition::Offset(offset(20)))],
            Duration::from_secs(30),
        )
        .unwrap_or_else(|error| panic!("replace assignment: {error:?}"));
    drain_effects(&mut owner);
    install_pending_ready(&mut owner, 20);

    let delivery = owner
        .take_delivery()
        .unwrap_or_else(|error| panic!("take active delivery: {error:?}"))
        .unwrap_or_else(|| panic!("active delivery after stale FIFO entry"));
    assert_eq!(delivery.fence().position().assignment_epoch(), active_epoch);
    assert_eq!(owner.fetches.retained().1, 1);
    owner
        .reclaim_delivery(delivery)
        .unwrap_or_else(|error| panic!("reclaim active delivery: {error:?}"));
    assert_eq!(owner.fetches.retained(), (0, 0, 0));
}

#[test]
fn ready_delivery_is_reclaimed_after_pause_or_seek_position_fence() {
    let mut paused = ready_owner();
    let epoch = paused
        .machine
        .assignment_epoch()
        .unwrap_or_else(|| panic!("active assignment"));
    let partition = paused.topics.partitions()[0].partition();
    paused
        .pause(epoch, partition)
        .unwrap_or_else(|error| panic!("pause: {error:?}"));
    drain_effects(&mut paused);
    assert!(
        paused
            .take_delivery()
            .unwrap_or_else(|error| panic!("reclaim paused delivery: {error:?}"))
            .is_none()
    );

    let mut sought = ready_owner();
    let epoch = sought
        .machine
        .assignment_epoch()
        .unwrap_or_else(|| panic!("active assignment"));
    let partition = sought.topics.partitions()[0].partition();
    sought
        .seek(
            epoch,
            partition,
            StartPosition::Offset(offset(20)),
            Duration::from_secs(30),
        )
        .unwrap_or_else(|error| panic!("seek: {error:?}"));
    drain_effects(&mut sought);
    assert!(
        sought
            .take_delivery()
            .unwrap_or_else(|error| panic!("reclaim sought delivery: {error:?}"))
            .is_none()
    );
}

#[test]
fn delivery_without_an_active_assignment_faults_and_retains_the_lease() {
    let mut owner = ready_owner();
    owner.machine = AssignedConsumerMachine::new();

    assert!(owner.take_delivery().is_err());
    assert!(matches!(
        owner.fault.as_ref(),
        Some(AssignedConsumerOwnerFault::Delivery {
            error: kafka_client_core::AssignedConsumerMachineError::NoAssignment,
            ..
        })
    ));
    assert_eq!(
        owner.fault_kind(),
        Some(AssignedConsumerFaultKind::Delivery)
    );
    assert_eq!(owner.fetches.retained().1, 1);
}

#[test]
fn reclaim_remains_available_through_owner_and_fetch_faults() {
    let mut owner = ready_owner();
    let delivery = owner
        .take_delivery()
        .unwrap_or_else(|error| panic!("take delivery: {error:?}"))
        .unwrap_or_else(|| panic!("ready delivery"));
    owner.fault = Some(AssignedConsumerOwnerFault::Clock(
        crate::clock::ClockError::TickOverflow,
    ));
    owner
        .reclaim_delivery(delivery)
        .unwrap_or_else(|error| panic!("reclaim through owner fault: {error:?}"));
    assert_eq!(owner.fetches.retained(), (0, 0, 0));

    let mut owner = ready_owner();
    let delivery = owner
        .take_delivery()
        .unwrap_or_else(|error| panic!("take delivery: {error:?}"))
        .unwrap_or_else(|| panic!("ready delivery"));
    owner.fetches.install_fault_for_test();
    assert!(owner.reclaim_delivery(delivery).is_err());
    assert_eq!(owner.reclaim_faults.len(), 1);
    assert_eq!(owner.fetches.retained().1, 1);
}

pub(super) fn ready_owner() -> AssignedConsumerOwner {
    let mut owner = owner(2);
    owner
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Offset(offset(10)))],
            Duration::from_secs(30),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));
    assert_eq!(owner.interpret_front_effect(), FrontEffect::Interpreted);
    install_pending_ready(&mut owner, 10);
    owner
}

pub(super) fn install_pending_ready(owner: &mut AssignedConsumerOwner, record_offset: i64) {
    install_pending_ready_with_records(owner, encoded_data_batch_for_test(record_offset));
}

pub(super) fn install_pending_ready_with_records(
    owner: &mut AssignedConsumerOwner,
    records: bytes::Bytes,
) {
    let prepared = owner
        .pending_fetches
        .pop_front()
        .unwrap_or_else(|| panic!("prepared Fetch"));
    install_terminal_for_test(
        &mut owner.fetches,
        prepared,
        FetchTerminalFixture::Success(Some(records)),
    );
    let now = owner
        .clock
        .now()
        .unwrap_or_else(|error| panic!("observe now: {error}"));
    assert!(owner.poll_fetch_executor(now));
    drain_effects(owner);
}

fn drain_effects(owner: &mut AssignedConsumerOwner) {
    while !owner.effects.is_empty() {
        assert_eq!(owner.interpret_front_effect(), FrontEffect::Interpreted);
    }
}

fn offset(value: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(value).unwrap_or_else(|| panic!("nonnegative test offset"))
}
