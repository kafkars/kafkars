//! Prepared count lookup selection and admission failure scenarios.

use std::time::Duration;

use kafka_client_core::{ClassicGroupPhase, Moment};

use crate::{EngineConfig, driver::DriverOwner};

use super::{
    classic_group_join::ClassicGroupExecutionState,
    classic_group_join_settlement::ClassicGroupJoinSettlementTurn,
    classic_group_join_settlement_test::leader_join_terminal,
    classic_group_partition_count_submission::ClassicGroupPartitionCountSubmissionTurn,
};

#[test]
fn confirmed_leader_selects_its_first_exact_topic_before_driver_admission() {
    let (mut registry, group_id, identity) = leader_join_terminal();
    assert_eq!(
        registry.settle_one_classic_join(Moment::from_tick(1)),
        Ok(ClassicGroupJoinSettlementTurn::Progress)
    );
    assert_eq!(
        registry.settle_one_classic_join(Moment::from_tick(2)),
        Ok(ClassicGroupJoinSettlementTurn::Progress)
    );

    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("leader entry expected"));
    let prepared = entry
        .execution
        .prepared_partition_counts()
        .unwrap_or_else(|| panic!("prepared counts expected"));
    let topic_id = entry
        .catalog
        .topic_id("orders")
        .unwrap_or_else(|| panic!("orders topic identity"));

    assert_eq!(prepared.cycle(), identity.cycle());
    assert_eq!(prepared.next_topic(), Some(topic_id));
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::PreparedPartitionCounts(_)
    ));

    registry
        .recover_classic_calls_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("classic call recovery failed: {error:?}"));
    super::registry_test_support::stop_registry(&mut registry);
}

#[test]
fn closed_driver_admission_fails_the_count_cycle_instead_of_spinning() {
    let (mut registry, group_id, _identity) = leader_join_terminal();
    for now in [Moment::from_tick(1), Moment::from_tick(2)] {
        assert_eq!(
            registry.settle_one_classic_join(now),
            Ok(ClassicGroupJoinSettlementTurn::Progress)
        );
    }
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver build failed: {error}"));
    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown failed: {error}"));

    assert_eq!(
        registry.submit_one_classic_partition_count(&driver),
        Ok(ClassicGroupPartitionCountSubmissionTurn::Progress)
    );
    let entry = registry
        .entry(group_id)
        .unwrap_or_else(|| panic!("leader entry expected"));
    assert_eq!(entry.classic.machine().phase(), ClassicGroupPhase::Lost);
    assert!(entry.execution.is_idle());

    registry
        .recover_classic_calls_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("remaining recovery failed: {error:?}"));
    super::registry_test_support::stop_registry(&mut registry);
}
