//! Accepted partition-count ownership recovery scenarios.

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
fn driver_shutdown_returns_the_exact_count_cycle_to_core_failure() {
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
    assert!(matches!(
        registry
            .entry(group_id)
            .unwrap_or_else(|| panic!("leader entry expected"))
            .execution
            .borrow_execution_state(),
        ClassicGroupExecutionState::PartitionCountDriverOwned { .. }
    ));

    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown failed: {error}"));
    registry
        .recover_classic_partition_counts_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("count recovery failed: {error:?}"));

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
