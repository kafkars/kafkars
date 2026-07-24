//! Bounded-turn progress reporting scenarios.

use std::time::Duration;

use kafka_client_core::{NextFetchOffset, StartPosition};

use super::{
    assigned_owner_effect::FrontEffect,
    assigned_owner_test::{driver, input, owner, shutdown},
    fetch_execution::{FetchTerminalFixture, install_terminal_for_test},
};

#[test]
fn one_interpreted_effect_ends_the_turn_before_submission() {
    let mut owner = owner(1);
    owner
        .replace_assignment(
            vec![input("orders", 0, StartPosition::Beginning)],
            Duration::from_secs(30),
        )
        .unwrap_or_else(|error| panic!("assign: {error:?}"));
    let mut driver = driver();

    let turn = owner.turn(&driver);
    assert!(turn.effect_interpreted);
    assert!(!turn.position_submitted);
    assert_eq!(owner.pending_positions.len(), 1);
    assert_eq!(owner.interpret_front_effect(), FrontEffect::Idle);
    shutdown(&mut driver);
}

#[test]
fn unresolved_control_pending_reports_no_turn_progress_until_restored() {
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
    owner
        .pause(epoch, fence.position().partition())
        .unwrap_or_else(|error| panic!("pause: {error:?}"));
    let exact_head = owner.effects.front().copied();
    let mut driver = driver();

    let blocked = owner.turn(&driver);
    assert!(!blocked.fetch_polled);
    assert!(!blocked.effect_interpreted);
    assert_eq!(owner.effects.front().copied(), exact_head);

    owner
        .fetches
        .tracked_calls_for_test()
        .restore_fetch_settlement(settlement)
        .unwrap_or_else(|failure| panic!("restore settlement: {:?}", failure.into_parts().1));
    let progressed = owner.turn(&driver);
    assert!(progressed.effect_interpreted);
    shutdown(&mut driver);
}

fn offset(value: i64) -> NextFetchOffset {
    NextFetchOffset::try_from_raw(value).unwrap_or_else(|| panic!("nonnegative offset"))
}
