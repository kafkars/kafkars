//! Sequential missing-offset reset execution scenarios.

use kafka_client_core::{
    ClassicGroupInput, ClassicGroupPhase, Deadline, GroupPositionBatch,
    GroupPositionBootstrapEffect, GroupPositionBootstrapInput, GroupPositionBootstrapMachine,
    GroupPositionMissingOffsetPolicy, GroupPositionPartitionFact, GroupPositionResetState,
    GroupPositionResetTerminal, Moment, PositionResolutionAttemptFailure, StartPosition,
};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_fetch::current_consumer_group_position_fence,
    classic_group_heartbeat::ClassicHeartbeatExecutionState,
    classic_group_heartbeat_rejection::install_heartbeat_rejection,
    classic_group_position::{
        ClassicGroupPositionCloseTurn, ClassicGroupPositionCompleted,
        ClassicGroupPositionExecutionState, ClassicGroupPositionFailure,
        ClassicGroupPositionSettlementTurn, close_entry_position,
        settlement_test_support::{
            PartitionValue, driver_owned_fixture_with_policy, install_legacy_terminal,
        },
    },
    classic_group_position_reset::ClassicGroupPositionResetTurn,
    consumer_group_assignment_retirement::{
        ConsumerGroupAssignmentRetirementTurn, retire_entry_assignment,
        stage_consumer_group_revocation,
    },
    consumer_group_heartbeat_settlement_test::installed_modern_entry,
    registry::GroupConsumerRegistry,
    registry_test_support::stop_registry,
};
use crate::clock::{MonotonicClock, OperationDeadline};

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
fn modern_reset_uses_the_consumer_group_assignment_fence() {
    let (mut entry, _topic_id) = installed_modern_entry();
    let consumer = entry
        .consumer
        .as_ref()
        .unwrap_or_else(|| panic!("modern execution"));
    let fence = current_consumer_group_position_fence(consumer, &entry.catalog)
        .unwrap_or_else(|error| panic!("modern fence: {error:?}"));
    let partition = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("modern assignment"))
        .partitions()[0];
    let mut bootstrap = GroupPositionBootstrapMachine::try_new_with_policy(
        fence,
        Deadline::from_tick(100),
        vec![partition],
        GroupPositionMissingOffsetPolicy::Earliest,
    )
    .unwrap_or_else(|error| panic!("modern position bootstrap: {error}"));
    bootstrap
        .apply(GroupPositionBootstrapInput::Start {
            fence,
            now: Moment::from_tick(1),
        })
        .and_then(|_| bootstrap.apply(GroupPositionBootstrapInput::DriverAccepted { fence }))
        .unwrap_or_else(|error| panic!("modern position submission: {error}"));
    let transition = bootstrap
        .apply(GroupPositionBootstrapInput::OffsetsFetched {
            fence,
            now: Moment::from_tick(50),
            batch: GroupPositionBatch::new(0, vec![GroupPositionPartitionFact::missing(partition)]),
        })
        .unwrap_or_else(|error| panic!("modern position terminal: {error}"));
    let Some(GroupPositionBootstrapEffect::Complete { terminal, .. }) = transition.into_effect()
    else {
        panic!("modern reset-required completion");
    };
    entry
        .position
        .set(ClassicGroupPositionExecutionState::Complete(
            ClassicGroupPositionCompleted::new_with_operation_deadline(
                bootstrap,
                terminal,
                Moment::from_tick(50),
                OperationDeadline::from_core_for_test(Deadline::from_tick(100)),
            ),
        ));
    let mut registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("registry: {error:?}"));
    registry.retained_group_bytes = entry.group_bytes();
    registry.entries.push(entry);

    assert_eq!(
        registry.begin_one_classic_group_position_reset(Moment::from_tick(51)),
        Ok(ClassicGroupPositionResetTurn::Progress)
    );
    let entry = registry
        .entries
        .first_mut()
        .unwrap_or_else(|| panic!("modern entry"));
    let ClassicGroupPositionExecutionState::ResetPrepared(prepared) = entry.position.state() else {
        panic!("modern reset lookup must be prepared");
    };
    assert_eq!(prepared.reset.fence(), fence);
    assert_eq!(
        close_entry_position(entry, Moment::from_tick(52)),
        Ok(ClassicGroupPositionCloseTurn::Progress)
    );
    let revoked = entry
        .consumer
        .as_mut()
        .unwrap_or_else(|| panic!("modern execution"))
        .close_locally()
        .unwrap_or_else(|error| panic!("modern close: {error:?}"));
    stage_consumer_group_revocation(entry, revoked)
        .unwrap_or_else(|error| panic!("modern revocation: {error:?}"));
    assert_eq!(
        retire_entry_assignment(entry, Moment::from_tick(53), &MonotonicClock::new()),
        Ok(ConsumerGroupAssignmentRetirementTurn::Progress)
    );
    stop_registry(&mut registry);
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

#[test]
fn heartbeat_loss_defers_a_completed_reset_to_membership_retirement() {
    let mut fixture =
        driver_owned_fixture_with_policy(&[0], GroupPositionMissingOffsetPolicy::Earliest);
    install_legacy_terminal(&mut fixture, Some(7), 0, 0, &[(0, PartitionValue::Missing)]);
    for _ in 0..2 {
        assert_eq!(
            fixture
                .registry
                .settle_one_classic_group_position(Moment::from_tick(50)),
            Ok(ClassicGroupPositionSettlementTurn::Progress)
        );
    }
    let entry = &mut fixture.registry.entries[0];
    let ClassicHeartbeatExecutionState::Waiting(schedule) = entry.heartbeat.state() else {
        panic!("heartbeat schedule");
    };
    let attempt = schedule.attempt();
    let now = Moment::from_tick(schedule.due().tick());
    entry
        .classic
        .apply(ClassicGroupInput::HeartbeatDue { attempt, now })
        .unwrap_or_else(|error| panic!("heartbeat due: {error}"));
    let transition = entry
        .classic
        .apply(ClassicGroupInput::HeartbeatFailed { attempt, now })
        .unwrap_or_else(|error| panic!("heartbeat failure: {error}"));
    install_heartbeat_rejection(entry, transition, now)
        .unwrap_or_else(|_error| panic!("membership retirement"));
    assert_eq!(
        entry.classic.machine().phase(),
        ClassicGroupPhase::WaitingToRejoin
    );
    assert!(entry.classic.machine().active_cycle().is_none());
    assert_eq!(
        fixture.registry.begin_one_classic_group_position_reset(now),
        Ok(ClassicGroupPositionResetTurn::Idle)
    );
    let entry = &mut fixture.registry.entries[0];
    assert!(matches!(
        entry.position.state(),
        ClassicGroupPositionExecutionState::Complete(_)
    ));
    assert!(entry.fault.is_none());
    assert_eq!(
        close_entry_position(entry, now),
        Ok(ClassicGroupPositionCloseTurn::Progress)
    );
    assert!(entry.position.is_dormant());
    stop_registry(&mut fixture.registry);
}
