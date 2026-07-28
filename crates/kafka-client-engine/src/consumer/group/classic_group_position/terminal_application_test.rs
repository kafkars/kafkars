//! Post-core terminal-shape mismatch ownership retention.

use kafka_client_core::{
    GroupPositionBootstrapEffect, GroupPositionBootstrapFetchFailure, GroupPositionBootstrapInput,
    GroupPositionBootstrapTerminal, MemberId, Moment,
};

use super::{
    super::registry_test_support::stop_registry, ClassicGroupPositionExecutionError,
    ClassicGroupPositionExecutionState, settlement_test_support::driver_owned_fixture,
    terminal_application::stage_terminal_effect,
};

#[test]
fn core_terminal_mismatch_freezes_applied_terminal_and_receipt() {
    let mut fixture = driver_owned_fixture(&[0]);
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
        .apply(GroupPositionBootstrapInput::FetchFailed {
            fence: fixture.fence,
            now: Moment::from_tick(50),
            failure: GroupPositionBootstrapFetchFailure::Transport,
        })
        .unwrap_or_else(|error| panic!("terminal transition: {error}"));
    let Some(GroupPositionBootstrapEffect::Complete {
        deadline, terminal, ..
    }) = transition.into_effect()
    else {
        panic!("complete effect expected");
    };
    let wrong_fence = kafka_client_core::GroupPositionFence::new(
        fixture.fence.group_id(),
        fixture.fence.membership_cycle(),
        MemberId::try_from_raw(99).unwrap_or_else(|| panic!("changed member")),
        fixture.fence.assignment_generation(),
    );
    let failure = stage_terminal_effect(
        &mut entry.position,
        fixture.fence,
        fixture.deadline.core(),
        fixture.deadline,
        Moment::from_tick(50),
        machine,
        correlation,
        accepted,
        Some(result_buffer),
        Some(GroupPositionBootstrapEffect::Complete {
            fence: wrong_fence,
            deadline,
            terminal,
        }),
    )
    .err()
    .unwrap_or_else(|| panic!("mismatched core effect must freeze"));
    assert_eq!(
        failure.error(),
        ClassicGroupPositionExecutionError::CompletionFence
    );
    assert!(!failure.raw_terminal_is_restorable());
    assert!(matches!(
        entry.position.state(),
        ClassicGroupPositionExecutionState::ConfirmationPending(pending)
            if pending.accepted().fence() == fixture.fence
                && matches!(
                    pending.completed().terminal(),
                    GroupPositionBootstrapTerminal::Failed(_)
                )
    ));
    drop(
        entry
            .position
            .replace(ClassicGroupPositionExecutionState::Dormant),
    );
    stop_registry(&mut fixture.registry);
}
