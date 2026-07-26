//! Accepted partition-count terminal settlement scenarios.

use std::time::Duration;

use kafka_client_core::{ClassicGroupPhase, Moment};

use crate::{
    EngineConfig,
    driver::{DriverOwner, TopicPartitionCountFact},
};

use super::{
    classic_group_join::ClassicGroupExecutionState,
    classic_group_join_settlement::ClassicGroupJoinSettlementTurn,
    classic_group_join_settlement_test::{empty_leader_join_terminal, leader_join_terminal},
    classic_group_partition_count_settlement::{
        ClassicGroupPartitionCountSettlementTurn, settle_count_fact,
    },
    classic_group_partition_count_submission::ClassicGroupPartitionCountSubmissionTurn,
    classic_group_sync_submission::ClassicGroupSyncSubmissionTurn,
    registry_membership::GroupConsumerMembershipTurn,
};

#[test]
fn terminal_driver_failure_settles_the_count_cycle_once() {
    let (mut registry, group_id, _identity) = leader_join_terminal();
    for now in [Moment::from_tick(1), Moment::from_tick(2)] {
        assert_eq!(
            registry.settle_one_classic_join(now),
            Ok(ClassicGroupJoinSettlementTurn::Progress)
        );
    }
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver build failed: {error}"));
    assert_eq!(
        registry.submit_one_classic_partition_count(&driver),
        Ok(ClassicGroupPartitionCountSubmissionTurn::Progress)
    );
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown failed: {error}"));

    assert_eq!(
        registry.settle_one_classic_partition_count(Moment::from_tick(3)),
        Ok(ClassicGroupPartitionCountSettlementTurn::Progress)
    );
    assert_eq!(
        registry.settle_one_classic_partition_count(Moment::from_tick(4)),
        Ok(ClassicGroupPartitionCountSettlementTurn::Idle)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("leader entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Lost);
    assert!(entry.execution.is_idle());
    assert!(entry.fault.is_none());

    registry
        .recover_classic_calls_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("remaining recovery failed: {error:?}"));
    super::registry_test_support::stop_registry(&mut registry);
}

#[test]
fn active_empty_topic_union_materializes_sync_without_a_topic_view_call() {
    let (mut registry, group_id, _identity) = empty_leader_join_terminal();
    settle_join(&mut registry);
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver build failed: {error}"));

    assert_eq!(
        registry.submit_one_classic_partition_count(&driver),
        Ok(ClassicGroupPartitionCountSubmissionTurn::Idle)
    );
    assert_eq!(
        registry.settle_one_classic_partition_count(Moment::from_tick(3)),
        Ok(ClassicGroupPartitionCountSettlementTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("leader entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Syncing);
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::PreparedSync(_)
    ));

    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown failed: {error}"));
    registry
        .recover_classic_calls_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("classic recovery failed: {error:?}"));
    super::registry_test_support::stop_registry(&mut registry);
}

#[test]
fn closing_empty_topic_union_closes_before_sync_materialization() {
    let (mut registry, group_id, _identity) = empty_leader_join_terminal();
    settle_join(&mut registry);
    registry
        .close_group(group_id)
        .unwrap_or_else(|error| panic!("group close failed: {error:?}"));

    assert_eq!(
        registry.settle_one_classic_partition_count(Moment::from_tick(3)),
        Ok(ClassicGroupPartitionCountSettlementTurn::Idle)
    );
    assert_eq!(
        registry.turn_local_membership(Moment::from_tick(3)),
        Ok(GroupConsumerMembershipTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("leader entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Closed);
    assert!(entry.execution.is_idle());
    super::registry_test_support::stop_registry(&mut registry);
}

#[test]
fn exact_deadline_empty_topic_union_expires_before_sync_materialization() {
    let (mut registry, group_id, identity) = empty_leader_join_terminal();
    settle_join(&mut registry);
    let now = Moment::from_tick(identity.deadline().core().tick());

    assert_eq!(
        registry.settle_one_classic_partition_count(now),
        Ok(ClassicGroupPartitionCountSettlementTurn::Idle)
    );
    assert_eq!(
        registry.turn_local_membership(now),
        Ok(GroupConsumerMembershipTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("leader entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Lost);
    assert!(entry.execution.is_idle());
    super::registry_test_support::stop_registry(&mut registry);
}

#[test]
fn close_after_count_handoff_success_never_materializes_sync() {
    let (mut registry, group_id, _identity) = leader_join_terminal();
    settle_join(&mut registry);
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver build failed: {error}"));
    assert_eq!(
        registry.submit_one_classic_partition_count(&driver),
        Ok(ClassicGroupPartitionCountSubmissionTurn::Progress)
    );
    registry
        .close_group(group_id)
        .unwrap_or_else(|error| panic!("group close failed: {error:?}"));
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown failed: {error}"));
    {
        let entry = registry
            .entries
            .iter_mut()
            .find(|entry| entry.group_id() == group_id)
            .unwrap_or_else(|| panic!("leader entry expected"));
        let state = entry
            .execution
            .replace_execution_state(ClassicGroupExecutionState::Idle);
        let ClassicGroupExecutionState::PartitionCountDriverOwned { prepared, mut call } = state
        else {
            panic!("accepted count call expected");
        };
        let identity = call.identity();
        assert!(call.try_terminal().is_some());
        assert_eq!(
            settle_count_fact(
                entry,
                prepared,
                call,
                identity,
                TopicPartitionCountFact {
                    metadata_generation: 11,
                    logical_partition_count: 4,
                },
                Moment::from_tick(3),
            ),
            Ok(ClassicGroupPartitionCountSettlementTurn::Progress)
        );
        assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Lost);
        assert!(entry.execution.is_idle());
    }
    assert_eq!(
        registry.submit_one_classic_sync(&driver),
        Ok(ClassicGroupSyncSubmissionTurn::Idle)
    );
    assert_eq!(
        registry.turn_local_membership(Moment::from_tick(4)),
        Ok(GroupConsumerMembershipTurn::Progress)
    );
    registry
        .recover_classic_calls_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("classic recovery failed: {error:?}"));
    super::registry_test_support::stop_registry(&mut registry);
}

fn settle_join(registry: &mut super::registry::GroupConsumerRegistry) {
    for now in [Moment::from_tick(1), Moment::from_tick(2)] {
        assert_eq!(
            registry.settle_one_classic_join(now),
            Ok(ClassicGroupJoinSettlementTurn::Progress)
        );
    }
}
