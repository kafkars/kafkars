//! Sequential missing-offset reset execution scenarios.

use kafka_client_core::{
    GroupPositionMissingOffsetPolicy, GroupPositionResetState, GroupPositionResetTerminal, Moment,
    PositionResolutionAttemptFailure, StartPosition,
};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_position::{
        ClassicGroupPositionExecutionState, ClassicGroupPositionFailure,
        ClassicGroupPositionSettlementTurn,
        settlement_test_support::{
            PartitionValue, driver_owned_fixture_with_policy, install_legacy_terminal,
        },
    },
    classic_group_position_reset::ClassicGroupPositionResetTurn,
    registry_test_support::stop_registry,
};

#[test]
fn confirmed_reset_terminal_becomes_one_exact_sequential_lookup() {
    for (policy, expected_position) in [
        (
            GroupPositionMissingOffsetPolicy::Earliest,
            StartPosition::Beginning,
        ),
        (GroupPositionMissingOffsetPolicy::Latest, StartPosition::End),
    ] {
        let mut fixture = driver_owned_fixture_with_policy(&[0, 1, 2], policy);
        install_legacy_terminal(
            &mut fixture,
            Some(7),
            11,
            0,
            &[
                (0, PartitionValue::Missing),
                (1, PartitionValue::Committed(5)),
                (2, PartitionValue::Missing),
            ],
        );
        assert_eq!(
            fixture
                .registry
                .settle_one_classic_group_position(Moment::from_tick(50)),
            Ok(ClassicGroupPositionSettlementTurn::Progress)
        );
        assert_eq!(
            fixture
                .registry
                .settle_one_classic_group_position(Moment::from_tick(50)),
            Ok(ClassicGroupPositionSettlementTurn::Progress)
        );

        assert_eq!(
            fixture
                .registry
                .begin_one_classic_group_position_reset(Moment::from_tick(51)),
            Ok(ClassicGroupPositionResetTurn::Progress)
        );
        let entry = fixture
            .registry
            .entries
            .iter()
            .find(|entry| entry.group_id() == fixture.group_id)
            .unwrap_or_else(|| panic!("position entry expected"));
        let ClassicGroupPositionExecutionState::ResetPrepared(prepared) = entry.position.state()
        else {
            panic!("one reset lookup must be prepared");
        };
        assert_eq!(
            prepared.reset.state(),
            GroupPositionResetState::AwaitingDriver
        );
        assert_eq!(prepared.partition.partition().get(), 0);
        assert_eq!(prepared.position, expected_position);
        assert_eq!(prepared.operation_deadline.core(), fixture.deadline.core());

        stop_registry(&mut fixture.registry);
    }
}

#[test]
fn original_deadline_terminalizes_reset_before_list_offsets_admission() {
    let mut fixture =
        driver_owned_fixture_with_policy(&[0], GroupPositionMissingOffsetPolicy::Earliest);
    install_legacy_terminal(&mut fixture, Some(7), 0, 0, &[(0, PartitionValue::Missing)]);
    assert_eq!(
        fixture
            .registry
            .settle_one_classic_group_position(Moment::from_tick(50)),
        Ok(ClassicGroupPositionSettlementTurn::Progress)
    );
    assert_eq!(
        fixture
            .registry
            .settle_one_classic_group_position(Moment::from_tick(50)),
        Ok(ClassicGroupPositionSettlementTurn::Progress)
    );

    assert_eq!(
        fixture
            .registry
            .begin_one_classic_group_position_reset(Moment::from_tick(u64::MAX)),
        Ok(ClassicGroupPositionResetTurn::Progress)
    );
    let entry = fixture
        .registry
        .entry(fixture.group_id)
        .unwrap_or_else(|| panic!("position entry expected"));
    assert!(matches!(
        entry.position.state(),
        ClassicGroupPositionExecutionState::ResetComplete(_)
    ));
    assert!(
        fixture
            .registry
            .terminalize_one_classic_group_position_failure()
    );
    let entry = fixture
        .registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == fixture.group_id)
        .unwrap_or_else(|| panic!("position entry expected"));
    let Some(ClassicGroupEntryFault::PositionFailure(ClassicGroupPositionFailure::Reset(
        completed,
    ))) = entry.fault.take()
    else {
        panic!("reset terminal must become the entry fault");
    };
    assert!(matches!(
        completed.terminal(),
        GroupPositionResetTerminal::Failed(failure)
            if failure.failure() == PositionResolutionAttemptFailure::DeadlineElapsed
    ));

    stop_registry(&mut fixture.registry);
}
