//! Successful, missing, group-rejected, and partition-rejected settlements.

use kafka_client_core::{
    GroupPositionBootstrapFailureKind, GroupPositionBootstrapTerminal,
    GroupPositionMissingOffsetPolicy, GroupPositionPartitionResult, Moment, StartPosition,
};

use crate::{
    EngineConfig, clock::MonotonicClock, consumer::GroupConsumerPositionFailureKind,
    driver::DriverOwner,
};

use super::{
    super::{
        classic_group_entry_fault::ClassicGroupEntryFault,
        classic_group_position::ClassicGroupPositionFailure,
        classic_group_position::{ClassicGroupPositionSettlementTurn, GroupConsumerPositionTurn},
        registry_fetch::GroupConsumerFetchTurn,
        registry_test_support::stop_registry,
    },
    ClassicGroupPositionExecutionState,
    settlement_test_support::{
        PartitionValue, PositionSettlementFixture, driver_owned_fixture,
        driver_owned_fixture_with_policy, install_legacy_terminal, position_state,
    },
};

#[test]
fn success_preserves_request_order_offsets_and_throttle() {
    let mut fixture = driver_owned_fixture(&[0, 1]);
    install_legacy_terminal(
        &mut fixture,
        Some(7),
        19,
        0,
        &[
            (1, PartitionValue::Committed(42)),
            (0, PartitionValue::Committed(7)),
        ],
    );
    settle_and_confirm(&mut fixture);
    let ClassicGroupPositionExecutionState::Complete(completed) = position_state(&fixture) else {
        panic!("complete position expected");
    };
    let GroupPositionBootstrapTerminal::Ready(batch) = completed.terminal() else {
        panic!("ready position expected");
    };
    assert_eq!(completed.observed_at(), Moment::from_tick(50));
    assert_eq!(batch.throttle_time_ms(), 19);
    assert_eq!(batch.facts()[0].partition().partition().get(), 0);
    assert_eq!(batch.facts()[1].partition().partition().get(), 1);
    assert!(matches!(
        batch.facts()[0].result(),
        GroupPositionPartitionResult::Committed(offset) if offset.get() == 7
    ));
    assert!(matches!(
        batch.facts()[1].result(),
        GroupPositionPartitionResult::Committed(offset) if offset.get() == 42
    ));
    stop_registry(&mut fixture.registry);
}

#[test]
fn missing_offset_fails_the_ordered_batch_atomically() {
    let mut fixture = driver_owned_fixture(&[0, 1]);
    install_legacy_terminal(
        &mut fixture,
        Some(7),
        11,
        0,
        &[
            (0, PartitionValue::Committed(5)),
            (1, PartitionValue::Missing),
        ],
    );
    settle_and_confirm(&mut fixture);
    let ClassicGroupPositionExecutionState::Complete(completed) = position_state(&fixture) else {
        panic!("complete position expected");
    };
    let GroupPositionBootstrapTerminal::MissingOffsets(missing) = completed.terminal() else {
        panic!("missing position expected");
    };
    assert_eq!(missing.batch().throttle_time_ms(), 11);
    assert_eq!(missing.batch().facts().len(), 2);
    assert_eq!(missing.first_missing().partition().partition().get(), 1);
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
    let Some(ClassicGroupEntryFault::PositionFailure(ClassicGroupPositionFailure::Bootstrap(
        completed,
    ))) = entry.fault.take()
    else {
        panic!("missing-offset terminal must become the entry fault");
    };
    assert!(matches!(
        completed.terminal(),
        GroupPositionBootstrapTerminal::MissingOffsets(missing)
            if missing.first_missing().partition().partition().get() == 1
    ));
    stop_registry(&mut fixture.registry);
}

#[test]
fn confirmed_missing_offset_waits_for_position_terminalization_before_fetch() {
    let mut fixture = driver_owned_fixture(&[0]);
    install_legacy_terminal(&mut fixture, Some(7), 0, 0, &[(0, PartitionValue::Missing)]);
    assert_eq!(
        fixture
            .registry
            .settle_one_classic_group_position(Moment::from_tick(50)),
        Ok(ClassicGroupPositionSettlementTurn::Progress)
    );
    assert!(matches!(
        position_state(&fixture),
        ClassicGroupPositionExecutionState::ConfirmationPending(_)
    ));

    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));
    let clock = MonotonicClock::new();
    assert_eq!(
        fixture
            .registry
            .turn_position(Moment::from_tick(51), &driver),
        Ok(GroupConsumerPositionTurn::Progress)
    );
    assert!(matches!(
        position_state(&fixture),
        ClassicGroupPositionExecutionState::Complete(completed)
            if matches!(completed.terminal(), GroupPositionBootstrapTerminal::MissingOffsets(_))
    ));

    assert_eq!(
        fixture.registry.turn_fetch(&clock, &driver),
        Ok(GroupConsumerFetchTurn::Idle)
    );
    {
        let entry = fixture
            .registry
            .entry(fixture.group_id)
            .unwrap_or_else(|| panic!("position entry expected"));
        assert!(entry.fault.is_none());
        assert!(entry.fetch.activation().is_none());
    }

    assert_eq!(
        fixture
            .registry
            .turn_position(Moment::from_tick(52), &driver),
        Ok(GroupConsumerPositionTurn::Progress)
    );
    let entry = fixture
        .registry
        .entry(fixture.group_id)
        .unwrap_or_else(|| panic!("position entry expected"));
    assert_eq!(
        entry.position_failure_observation,
        Some(GroupConsumerPositionFailureKind::MissingOffset)
    );
    assert!(matches!(
        &entry.fault,
        Some(ClassicGroupEntryFault::PositionFailure(
            ClassicGroupPositionFailure::Bootstrap(_)
        ))
    ));
    let entry = fixture
        .registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == fixture.group_id)
        .unwrap_or_else(|| panic!("position entry expected"));
    drop(entry.fault.take());

    stop_registry(&mut fixture.registry);
}

#[test]
fn earliest_and_latest_retain_exact_reset_work_without_fetch_activation() {
    for (policy, expected) in [
        (
            GroupPositionMissingOffsetPolicy::Earliest,
            StartPosition::Beginning,
        ),
        (GroupPositionMissingOffsetPolicy::Latest, StartPosition::End),
    ] {
        let mut fixture = driver_owned_fixture_with_policy(&[0, 1], policy);
        install_legacy_terminal(
            &mut fixture,
            Some(7),
            11,
            0,
            &[
                (0, PartitionValue::Committed(5)),
                (1, PartitionValue::Missing),
            ],
        );
        settle_and_confirm(&mut fixture);
        let entry = fixture
            .registry
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == fixture.group_id)
            .unwrap_or_else(|| panic!("position entry expected"));
        let ClassicGroupPositionExecutionState::Complete(completed) = entry.position.state() else {
            panic!("reset-required position expected");
        };
        let GroupPositionBootstrapTerminal::ResetRequired(reset) = completed.terminal() else {
            panic!("explicit reset policy must not become Error");
        };
        assert_eq!(reset.position(), expected);
        assert_eq!(reset.first_missing().partition().partition().get(), 1);
        assert_eq!(
            super::super::classic_group_fetch::transfer_completed_position(
                &entry.classic,
                &entry.catalog,
                &mut entry.position,
                &mut entry.fetch,
            ),
            Ok(super::super::classic_group_fetch::ClassicGroupFetchTransferTurn::Idle)
        );
        assert!(entry.fetch.activation().is_none());
        stop_registry(&mut fixture.registry);
    }
}

#[test]
fn exact_signed_group_rejection_is_terminal() {
    let mut fixture = driver_owned_fixture(&[0]);
    install_legacy_terminal(&mut fixture, Some(7), 3, -911, &[]);
    settle_and_confirm(&mut fixture);
    let ClassicGroupPositionExecutionState::Complete(completed) = position_state(&fixture) else {
        panic!("complete position expected");
    };
    assert!(matches!(
        completed.terminal(),
        GroupPositionBootstrapTerminal::Failed(failure)
            if matches!(
                failure.kind(),
                GroupPositionBootstrapFailureKind::Broker(error) if error.code() == -911
            )
    ));
    stop_registry(&mut fixture.registry);
}

#[test]
fn exact_signed_partition_rejection_preserves_full_batch() {
    let mut fixture = driver_owned_fixture(&[0, 1]);
    install_legacy_terminal(
        &mut fixture,
        Some(7),
        7,
        0,
        &[
            (0, PartitionValue::Committed(8)),
            (1, PartitionValue::Rejected(-731)),
        ],
    );
    settle_and_confirm(&mut fixture);
    let ClassicGroupPositionExecutionState::Complete(completed) = position_state(&fixture) else {
        panic!("complete position expected");
    };
    let GroupPositionBootstrapTerminal::PartitionRejected(rejected) = completed.terminal() else {
        panic!("partition rejection expected");
    };
    assert_eq!(rejected.batch().throttle_time_ms(), 7);
    assert_eq!(rejected.batch().facts().len(), 2);
    assert!(matches!(
        rejected.first_rejected().result(),
        GroupPositionPartitionResult::Rejected(error) if error.code() == -731
    ));
    stop_registry(&mut fixture.registry);
}

fn settle_and_confirm(fixture: &mut PositionSettlementFixture) {
    assert_eq!(
        fixture
            .registry
            .settle_one_classic_group_position(Moment::from_tick(50)),
        Ok(ClassicGroupPositionSettlementTurn::Progress)
    );
    assert!(matches!(
        position_state(fixture),
        ClassicGroupPositionExecutionState::ConfirmationPending(_)
    ));
    assert_eq!(
        fixture
            .registry
            .settle_one_classic_group_position(Moment::from_tick(51)),
        Ok(ClassicGroupPositionSettlementTurn::Progress)
    );
}
