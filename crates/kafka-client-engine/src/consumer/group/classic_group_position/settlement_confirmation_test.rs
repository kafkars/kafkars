//! Exact route confirmation and failed-confirmation receipt retention.

use kafka_client_core::{GroupPositionFence, MemberId, Moment};

use crate::driver::{
    GroupPositionOffsetFetchAccepted, GroupPositionOffsetFetchKey,
    TrackedGroupPositionOffsetFetchCalls,
};

use super::{
    super::{
        classic_group_position::ClassicGroupPositionSettlementTurn,
        registry_test_support::stop_registry,
    },
    ClassicGroupPositionExecutionError, ClassicGroupPositionExecutionState,
    settlement_test_support::{
        PartitionValue, driver_owned_fixture, install_legacy_terminal, position_state,
    },
};

#[test]
fn exact_confirmation_consumes_route_and_receipt_then_installs_complete() {
    let mut fixture = driver_owned_fixture(&[0]);
    install_legacy_terminal(
        &mut fixture,
        Some(7),
        2,
        0,
        &[(0, PartitionValue::Committed(9))],
    );
    assert_eq!(
        fixture
            .registry
            .settle_one_classic_group_position(Moment::from_tick(50)),
        Ok(ClassicGroupPositionSettlementTurn::Progress)
    );
    assert!(matches!(
        position_state(&fixture),
        ClassicGroupPositionExecutionState::ConfirmationPending(pending)
            if pending.accepted().fence() == fixture.fence
    ));
    assert_eq!(
        fixture
            .registry
            .settle_one_classic_group_position(Moment::from_tick(51)),
        Ok(ClassicGroupPositionSettlementTurn::Progress)
    );
    assert!(matches!(
        position_state(&fixture),
        ClassicGroupPositionExecutionState::Complete(completed)
            if completed.fence() == fixture.fence
    ));
    assert_eq!(
        fixture
            .registry
            .position_calls
            .as_ref()
            .unwrap_or_else(|| panic!("position calls expected"))
            .retained_group_position_offset_fetch_count(),
        0
    );
    stop_registry(&mut fixture.registry);
}

#[test]
fn failed_confirmation_retains_receipt_and_confirmation_pending_state() {
    let mut fixture = driver_owned_fixture(&[0]);
    install_legacy_terminal(
        &mut fixture,
        Some(7),
        0,
        0,
        &[(0, PartitionValue::Committed(9))],
    );
    assert_eq!(
        fixture
            .registry
            .settle_one_classic_group_position(Moment::from_tick(50)),
        Ok(ClassicGroupPositionSettlementTurn::Progress)
    );
    let exact_calls = fixture
        .registry
        .position_calls
        .take()
        .unwrap_or_else(|| panic!("exact pending calls expected"));

    let wrong_fence = changed_member_fence(fixture.fence);
    let wrong_key = GroupPositionOffsetFetchKey::new(wrong_fence, fixture.deadline);
    let wrong_accepted = GroupPositionOffsetFetchAccepted::from_fence_for_test(wrong_fence);
    let mut wrong_calls = TrackedGroupPositionOffsetFetchCalls::new(1);
    wrong_calls.install_empty_terminal_for_test(wrong_key, Some(7));
    let wrong_terminal = wrong_calls
        .begin_group_position_offset_fetch_settlement(&wrong_accepted)
        .unwrap_or_else(|error| panic!("wrong pending setup: {error:?}"));

    let entry = fixture
        .registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == fixture.group_id)
        .unwrap_or_else(|| panic!("position entry expected"));
    assert_eq!(
        entry.position.confirm_terminal_settlement(&mut wrong_calls),
        Err(ClassicGroupPositionExecutionError::Confirmation)
    );
    assert!(matches!(
        entry.position.state(),
        ClassicGroupPositionExecutionState::ConfirmationPending(pending)
            if pending.accepted().fence() == fixture.fence
    ));

    wrong_calls
        .restore_group_position_offset_fetch_settlement(wrong_terminal)
        .unwrap_or_else(|failure| {
            let (_terminal, error) = failure.into_parts();
            panic!("restore wrong terminal: {error:?}");
        });
    drop(wrong_accepted);
    let mut recovery = wrong_calls.recover_group_position_offset_fetches_after_driver_shutdown();
    drop(
        recovery
            .take_settled()
            .unwrap_or_else(|| panic!("wrong settled terminal expected")),
    );
    assert!(recovery.is_empty());
    fixture.registry.position_calls = Some(exact_calls);
    assert_eq!(
        fixture
            .registry
            .settle_one_classic_group_position(Moment::from_tick(51)),
        Ok(ClassicGroupPositionSettlementTurn::Progress)
    );
    assert!(matches!(
        position_state(&fixture),
        ClassicGroupPositionExecutionState::Complete(_)
    ));
    stop_registry(&mut fixture.registry);
}

fn changed_member_fence(fence: GroupPositionFence) -> GroupPositionFence {
    GroupPositionFence::new(
        fence.group_id(),
        fence.membership_cycle(),
        MemberId::try_from_raw(99).unwrap_or_else(|| panic!("changed member")),
        fence.assignment_generation(),
    )
}
