//! FIFO effect preparation and deadline-provenance scenarios.

use std::time::Duration;

use kafka_client_core::{AssignedConsumerEffect, Deadline, NextFetchOffset, StartPosition};

use super::{
    assigned_close_error::AssignedCloseSlotPhase,
    assigned_owner_effect::FrontEffect,
    assigned_owner_test::{input, owner},
    fetch_execution::{FetchTerminalFixture, install_terminal_for_test},
};

#[test]
fn position_preparation_preserves_the_original_public_deadline() {
    let mut owner = owner(1);
    owner
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Beginning)],
            Duration::from_secs(30),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));
    let original = owner.raw_position_deadlines[0].deadline;

    assert_eq!(owner.interpret_front_effect(), FrontEffect::Interpreted);
    assert!(owner.effects.is_empty());
    assert_eq!(owner.pending_positions[0].deadline, original);
    assert_eq!(owner.next_deadline(), Some(original.core()));
}

#[test]
fn all_partition_effects_transfer_without_pending_queue_deadlock() {
    let mut owner = owner(3);
    owner
        .replace_assignment(
            vec![
                input("orders", 0, StartPosition::Beginning),
                input("orders", 1, StartPosition::Beginning),
                input("orders", 2, StartPosition::Offset(offset(20))),
            ],
            Duration::from_secs(30),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));

    while !owner.effects.is_empty() {
        assert_eq!(owner.interpret_front_effect(), FrontEffect::Interpreted);
    }
    assert_eq!(owner.pending_positions.len(), 2);
    assert_eq!(owner.pending_fetches.len(), 1);
}

#[test]
fn next_deadline_is_the_minimum_and_faults_disable_wake_scheduling() {
    let mut owner = owner(2);
    owner
        .replace_assignment(
            vec![
                input("orders", 0, StartPosition::Beginning),
                input("orders", 1, StartPosition::Offset(offset(20))),
            ],
            Duration::from_secs(30),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));
    while !owner.effects.is_empty() {
        assert_eq!(owner.interpret_front_effect(), FrontEffect::Interpreted);
    }
    let fence = owner.pending_positions[0].prepared.fence();
    owner
        .timers
        .arm_position(fence, Deadline::from_tick(1))
        .unwrap_or_else(|error| panic!("arm earlier timer: {error:?}"));
    assert_eq!(owner.next_deadline(), Some(Deadline::from_tick(1)));

    owner.fault = Some(
        super::assigned_owner_fault::AssignedConsumerOwnerFault::Clock(
            crate::clock::ClockError::TickOverflow,
        ),
    );
    assert_eq!(owner.next_deadline(), None);
}

#[test]
fn fetch_control_pending_keeps_the_exact_fifo_head_and_other_mechanisms_unchanged() {
    let mut owner = owner(1);
    let epoch = owner
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Offset(offset(10)))],
            Duration::from_secs(30),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));
    assert_eq!(owner.interpret_front_effect(), FrontEffect::Interpreted);
    let prepared = owner
        .pending_fetches
        .pop_front()
        .unwrap_or_else(|| panic!("prepared Fetch"));
    let fence = prepared.fence();
    install_terminal_for_test(
        &mut owner.fetches,
        prepared,
        FetchTerminalFixture::Success(None),
    );
    let settlement = owner
        .fetches
        .tracked_calls_for_test()
        .begin_fetch_settlement(fence)
        .unwrap_or_else(|error| panic!("begin pending confirmation: {error:?}"));
    let partition = fence.position().partition();
    owner
        .pause(epoch, partition)
        .unwrap_or_else(|error| panic!("pause: {error:?}"));
    let exact_front = owner.effects.front().copied();
    let timers = owner.timers.timer_count();

    assert_eq!(owner.interpret_front_effect(), FrontEffect::ControlPending);
    assert_eq!(owner.effects.front().copied(), exact_front);
    assert_eq!(owner.timers.timer_count(), timers);
    owner
        .fetches
        .tracked_calls_for_test()
        .restore_fetch_settlement(settlement)
        .unwrap_or_else(|failure| panic!("restore settlement: {:?}", failure.into_parts().1));
    assert_eq!(owner.interpret_front_effect(), FrontEffect::Interpreted);
}

#[test]
fn accepted_close_is_not_blocked_by_prepared_position_work() {
    let mut owner = owner(1);
    owner
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Beginning)],
            Duration::from_secs(30),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));
    assert_eq!(owner.interpret_front_effect(), FrontEffect::Interpreted);
    owner
        .begin_close()
        .unwrap_or_else(|error| panic!("begin close: {error:?}"));

    assert!(matches!(
        owner.effects.front(),
        Some(AssignedConsumerEffect::AcceptClose { .. })
    ));
    assert_eq!(owner.interpret_front_effect(), FrontEffect::Interpreted);
    assert_eq!(owner.close.phase(), AssignedCloseSlotPhase::Accepted);
    assert_eq!(owner.pending_positions.len(), 1);
}

fn offset(value: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(value).unwrap_or_else(|| panic!("nonnegative offset"))
}
