//! Bounded protocol, driver failure, deadline, and raw-restoration scenarios.

use kafka_client_core::{
    GroupPositionBootstrapFailureKind, GroupPositionBootstrapInput,
    GroupPositionBootstrapMachineError, GroupPositionBootstrapTerminal, Moment,
};

use crate::driver::GroupPositionOffsetFetchDriverFailureKind;

use super::{
    super::{
        classic_group_execution::ClassicGroupExecutionError,
        classic_group_position::ClassicGroupPositionSettlementTurn,
        registry_test_support::stop_registry,
    },
    CLASSIC_GROUP_POSITION_RESULT_RETAINED_BYTES, ClassicGroupPositionDriverOwned,
    ClassicGroupPositionExecutionError, ClassicGroupPositionExecutionState,
    settlement_test_support::{
        PartitionValue, PositionSettlementFixture, driver_owned_fixture, install_driver_failure,
        install_empty_terminal, install_legacy_terminal, position_state, release_restored_owners,
    },
};

#[test]
fn retained_response_overflow_becomes_response_too_large() {
    let mut fixture = driver_owned_fixture(&[0]);
    install_legacy_terminal(
        &mut fixture,
        Some(7),
        0,
        0,
        &[(
            0,
            PartitionValue::CommittedWithMetadataBytes {
                offset: 3,
                bytes: CLASSIC_GROUP_POSITION_RESULT_RETAINED_BYTES + 1,
            },
        )],
    );
    settle_and_confirm_failure(
        &mut fixture,
        Moment::from_tick(50),
        GroupPositionBootstrapFailureKind::ResponseTooLarge,
    );
}

#[test]
fn driver_compatibility_invalid_response_and_transport_remain_distinct() {
    let cases = [
        (
            GroupPositionOffsetFetchDriverFailureKind::Compatibility,
            GroupPositionBootstrapFailureKind::Compatibility,
        ),
        (
            GroupPositionOffsetFetchDriverFailureKind::InvalidResponse,
            GroupPositionBootstrapFailureKind::InvalidResponse,
        ),
        (
            GroupPositionOffsetFetchDriverFailureKind::Transport,
            GroupPositionBootstrapFailureKind::Transport,
        ),
    ];
    for (kind, expected) in cases {
        let mut fixture = driver_owned_fixture(&[0]);
        install_driver_failure(&mut fixture, kind);
        settle_and_confirm_failure(&mut fixture, Moment::from_tick(50), expected);
    }
}

#[test]
fn driver_deadline_and_due_transport_both_use_deadline_precedence() {
    let kinds = [
        GroupPositionOffsetFetchDriverFailureKind::DeadlineElapsed,
        GroupPositionOffsetFetchDriverFailureKind::Transport,
    ];
    for kind in kinds {
        let mut fixture = driver_owned_fixture(&[0]);
        install_driver_failure(&mut fixture, kind);
        settle_and_confirm_failure(
            &mut fixture,
            Moment::from_tick(100),
            GroupPositionBootstrapFailureKind::DeadlineElapsed,
        );
    }
}

#[test]
fn malformed_protocol_response_is_an_invalid_response_terminal() {
    let mut fixture = driver_owned_fixture(&[0]);
    install_empty_terminal(&mut fixture, Some(7));
    settle_and_confirm_failure(
        &mut fixture,
        Moment::from_tick(50),
        GroupPositionBootstrapFailureKind::InvalidResponse,
    );
}

#[test]
fn core_rejection_restores_exact_raw_terminal_and_driver_owner() {
    let mut fixture = driver_owned_fixture(&[0]);
    force_completed_core_under_driver_owner(&mut fixture);
    install_empty_terminal(&mut fixture, Some(7));

    assert_eq!(
        fixture
            .registry
            .settle_one_classic_group_position(Moment::from_tick(50)),
        Err(ClassicGroupExecutionError::Position(
            ClassicGroupPositionExecutionError::Core(
                GroupPositionBootstrapMachineError::AlreadyCompleted
            )
        ))
    );
    assert!(matches!(
        position_state(&fixture),
        ClassicGroupPositionExecutionState::DriverOwned(owner)
            if owner.accepted().fence() == fixture.fence
    ));
    assert!(matches!(
        fixture
            .registry
            .position_calls
            .as_mut()
            .unwrap_or_else(|| panic!("position calls expected"))
            .poll_group_position_offset_fetch(),
        Ok(crate::driver::GroupPositionOffsetFetchPoll::TerminalReady { fence })
            if fence == fixture.fence
    ));
    release_restored_owners(&mut fixture);
    stop_registry(&mut fixture.registry);
}

fn settle_and_confirm_failure(
    fixture: &mut PositionSettlementFixture,
    now: Moment,
    expected: GroupPositionBootstrapFailureKind,
) {
    assert_eq!(
        fixture.registry.settle_one_classic_group_position(now),
        Ok(ClassicGroupPositionSettlementTurn::Progress)
    );
    assert_eq!(
        fixture.registry.settle_one_classic_group_position(now),
        Ok(ClassicGroupPositionSettlementTurn::Progress)
    );
    let ClassicGroupPositionExecutionState::Complete(completed) = position_state(fixture) else {
        panic!("complete position expected");
    };
    assert!(matches!(
        completed.terminal(),
        GroupPositionBootstrapTerminal::Failed(failure) if failure.kind() == expected
    ));
    stop_registry(&mut fixture.registry);
}

fn force_completed_core_under_driver_owner(fixture: &mut PositionSettlementFixture) {
    let entry = fixture
        .registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == fixture.group_id)
        .unwrap_or_else(|| panic!("position entry expected"));
    let state = entry
        .position
        .replace(ClassicGroupPositionExecutionState::Dormant);
    let ClassicGroupPositionExecutionState::DriverOwned(owner) = state else {
        panic!("driver-owned position expected");
    };
    let (mut machine, correlation, accepted, result_buffer) = owner.into_parts();
    let transition = machine
        .apply(GroupPositionBootstrapInput::DeadlineElapsed {
            fence: fixture.fence,
            now: Moment::from_tick(100),
        })
        .unwrap_or_else(|error| panic!("forced completion: {error}"));
    drop(transition);
    entry
        .position
        .set(ClassicGroupPositionExecutionState::DriverOwned(
            ClassicGroupPositionDriverOwned::new(machine, correlation, accepted, result_buffer),
        ));
}
