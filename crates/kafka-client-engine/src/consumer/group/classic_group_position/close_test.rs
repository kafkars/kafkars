//! Prepared, completed, and post-driver position close ownership.

use kafka_client_core::{
    AssignmentGeneration, Deadline, GroupPositionBootstrapEffect, GroupPositionBootstrapInput,
    GroupPositionBootstrapMachine, GroupPositionFence, MemberId, Moment,
};

use super::{
    super::{
        classic_group_position::ClassicGroupPositionSettlementTurn,
        classic_group_rejoin_due::ClassicGroupRejoinDueTurn,
        classic_group_rejoin_test_support::{arm_rejoin, entry_mut},
        registry_membership::GroupConsumerMembershipTurn,
        registry_test_support::{install_session, register, started_registry, stop_registry},
    },
    ClassicGroupPositionCloseTurn, ClassicGroupPositionCompleted, ClassicGroupPositionExecution,
    ClassicGroupPositionExecutionState, ClassicGroupPositionPreparation,
    prepare_classic_group_position,
    settlement_test_support::{
        PartitionValue, driver_owned_fixture, install_legacy_terminal, position_state,
        prepared_fixture,
    },
};

#[test]
fn dormant_close_is_an_exact_no_op() {
    let mut execution = ClassicGroupPositionExecution::new();
    assert!(matches!(
        execution.close_position_if_local(Moment::from_tick(1)),
        Ok(ClassicGroupPositionCloseTurn::Idle)
    ));
    assert!(matches!(
        execution.state(),
        ClassicGroupPositionExecutionState::Dormant
    ));
}

#[test]
fn prepared_close_applies_deadline_precedence_then_drops_complete_terminal() {
    let mut fixture = prepared_fixture(&[0]);
    fixture
        .registry
        .close_group(fixture.group_id)
        .unwrap_or_else(|error| panic!("close group: {error:?}"));
    assert_eq!(
        fixture
            .registry
            .turn_local_membership(Moment::from_tick(u64::MAX)),
        Ok(GroupConsumerMembershipTurn::Progress)
    );
    assert!(matches!(
        position_state(&fixture),
        ClassicGroupPositionExecutionState::Dormant
    ));
    stop_registry(&mut fixture.registry);
}

#[test]
fn complete_close_explicitly_consumes_the_internal_terminal() {
    let mut fixture = driver_owned_fixture(&[0]);
    install_legacy_terminal(
        &mut fixture,
        Some(7),
        0,
        0,
        &[(0, PartitionValue::Committed(4))],
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
            .settle_one_classic_group_position(Moment::from_tick(51)),
        Ok(ClassicGroupPositionSettlementTurn::Progress)
    );
    fixture
        .registry
        .close_group(fixture.group_id)
        .unwrap_or_else(|error| panic!("close group: {error:?}"));
    assert_eq!(
        fixture
            .registry
            .turn_local_membership(Moment::from_tick(52)),
        Ok(GroupConsumerMembershipTurn::Progress)
    );
    assert!(matches!(
        position_state(&fixture),
        ClassicGroupPositionExecutionState::Dormant
    ));
    stop_registry(&mut fixture.registry);
}

#[test]
fn driver_shutdown_recovery_stages_complete_then_local_close_consumes_it() {
    let mut fixture = driver_owned_fixture(&[0]);
    install_legacy_terminal(
        &mut fixture,
        Some(7),
        0,
        0,
        &[(0, PartitionValue::Committed(4))],
    );
    fixture
        .registry
        .recover_classic_group_positions_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("position recovery: {error:?}"));
    assert!(matches!(
        position_state(&fixture),
        ClassicGroupPositionExecutionState::Complete(_)
    ));
    fixture
        .registry
        .close_group(fixture.group_id)
        .unwrap_or_else(|error| panic!("close group: {error:?}"));
    assert_eq!(
        fixture
            .registry
            .turn_local_membership(Moment::from_tick(u64::MAX)),
        Ok(GroupConsumerMembershipTurn::Progress)
    );
    assert!(matches!(
        position_state(&fixture),
        ClassicGroupPositionExecutionState::Dormant
    ));
    stop_registry(&mut fixture.registry);
}

#[test]
fn completed_lost_assignment_retires_before_the_next_rejoin_cycle() {
    let mut registry = started_registry();
    let group_id = register(&mut registry, "workers");
    let schedule = arm_rejoin(&mut registry, group_id, 10);
    install_completed_position(&mut registry, group_id, schedule.cycle().get());

    assert_eq!(
        registry.turn_local_membership(Moment::from_tick(schedule.due().tick())),
        Ok(GroupConsumerMembershipTurn::Progress)
    );
    assert!(entry_mut(&mut registry, group_id).position.is_dormant());
    assert_eq!(
        registry.prepare_one_classic_rejoin(
            Moment::from_tick(schedule.due().tick()),
            &crate::clock::MonotonicClock::new(),
        ),
        Ok(ClassicGroupRejoinDueTurn::Progress)
    );
    stop_registry(&mut registry);
}

#[test]
fn shutdown_recovery_quiesces_two_position_and_two_membership_actions() {
    let mut registry = started_registry();
    let first = register(&mut registry, "first");
    let second = register(&mut registry, "second");
    install_session(&mut registry, first);
    install_session(&mut registry, second);
    install_prepared_position(&mut registry, first, 100);
    install_prepared_position(&mut registry, second, 101);
    assert_eq!(registry.position_unsettled(), 2);

    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("whole registry recovery: {error}"));
    assert_eq!(registry.membership_unsettled(), 0);
    assert_eq!(registry.position_unsettled(), 0);
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("finish recovered registry: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}

fn install_prepared_position(
    registry: &mut super::super::registry::GroupConsumerRegistry,
    group_id: kafka_client_core::GroupId,
    deadline: u64,
) {
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("entry expected"));
    let cycle = entry
        .classic
        .machine()
        .active_cycle()
        .unwrap_or_else(|| panic!("stable membership cycle expected"));
    let assignment = entry
        .catalog
        .live_assignment()
        .unwrap_or_else(|| panic!("live assignment expected"));
    let prepared = prepare_classic_group_position(
        &entry.catalog,
        cycle,
        assignment,
        crate::clock::OperationDeadline::from_core_for_test(Deadline::from_tick(deadline)),
        Moment::from_tick(5),
    )
    .unwrap_or_else(|error| panic!("position preparation: {error:?}"));
    let ClassicGroupPositionPreparation::Prepared(prepared) = prepared else {
        panic!("assigned position request expected");
    };
    entry
        .position
        .set(ClassicGroupPositionExecutionState::Prepared(prepared));
}

fn install_completed_position(
    registry: &mut super::super::registry::GroupConsumerRegistry,
    group_id: kafka_client_core::GroupId,
    cycle: u64,
) {
    let fence = GroupPositionFence::new(
        group_id,
        kafka_client_core::MembershipCycle::try_from_raw(cycle)
            .unwrap_or_else(|| panic!("nonzero cycle expected")),
        MemberId::try_from_raw(1).unwrap_or_else(|| panic!("member expected")),
        AssignmentGeneration::try_from_raw(1)
            .unwrap_or_else(|| panic!("assignment generation expected")),
    );
    let mut machine =
        GroupPositionBootstrapMachine::try_new(fence, Deadline::from_tick(100), Vec::new())
            .unwrap_or_else(|error| panic!("empty bootstrap: {error}"));
    let transition = machine
        .apply(GroupPositionBootstrapInput::Start {
            fence,
            now: Moment::from_tick(1),
        })
        .unwrap_or_else(|error| panic!("empty bootstrap start: {error}"));
    let Some(GroupPositionBootstrapEffect::Complete { terminal, .. }) = transition.into_effect()
    else {
        panic!("empty bootstrap completion expected");
    };
    entry_mut(registry, group_id)
        .position
        .set(ClassicGroupPositionExecutionState::Complete(
            ClassicGroupPositionCompleted::new(machine, terminal),
        ));
}
