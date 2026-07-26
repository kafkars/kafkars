//! Partition-count mechanism failure classification scenarios.

use std::time::Duration;

use kafka_client_core::{Deadline, Moment};

use crate::{
    EngineConfig,
    driver::{DriverOwner, TopicPartitionCountFailure},
};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_join::ClassicGroupExecutionState,
    classic_group_join_settlement::ClassicGroupJoinSettlementTurn,
    classic_group_join_settlement_test::leader_join_terminal,
    classic_group_partition_count_failure::{
        ClassicGroupPartitionCountFailureDisposition, ClassicGroupPartitionCountFault,
        classify_partition_count_failure, expire_count_cycle,
    },
    classic_group_partition_count_submission::ClassicGroupPartitionCountSubmissionTurn,
};

#[test]
fn accepted_capacity_fails_once_while_completion_corruption_faults() {
    assert_eq!(
        classify_partition_count_failure(TopicPartitionCountFailure::QueryCapacity(3), false),
        ClassicGroupPartitionCountFailureDisposition::CycleFailed
    );
    assert_eq!(
        classify_partition_count_failure(
            TopicPartitionCountFailure::Capacity {
                call_limit: 4,
                byte_limit: 64,
            },
            false
        ),
        ClassicGroupPartitionCountFailureDisposition::CycleFailed
    );
    assert_eq!(
        classify_partition_count_failure(TopicPartitionCountFailure::TopicMismatch, false),
        ClassicGroupPartitionCountFailureDisposition::Fault
    );
    assert_eq!(
        classify_partition_count_failure(TopicPartitionCountFailure::Unavailable, false),
        ClassicGroupPartitionCountFailureDisposition::CycleFailed
    );
}

#[test]
fn exact_deadline_precedes_every_non_corrupt_terminal_failure() {
    let deadline = Deadline::from_tick(7);
    let elapsed = deadline.is_elapsed_at(Moment::from_tick(7));

    assert_eq!(
        classify_partition_count_failure(TopicPartitionCountFailure::QueryCapacity(3), elapsed),
        ClassicGroupPartitionCountFailureDisposition::DeadlineElapsed
    );
    assert_eq!(
        classify_partition_count_failure(TopicPartitionCountFailure::Broker(17), elapsed),
        ClassicGroupPartitionCountFailureDisposition::DeadlineElapsed
    );
    assert_eq!(
        classify_partition_count_failure(TopicPartitionCountFailure::Completion, elapsed),
        ClassicGroupPartitionCountFailureDisposition::Fault
    );
}

#[test]
fn rejected_deadline_transition_freezes_the_exact_accepted_call_owner() {
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
    let entry = registry
        .entries
        .iter_mut()
        .find(|entry| entry.group_id() == group_id)
        .unwrap_or_else(|| panic!("leader entry expected"));
    let state = entry
        .execution
        .replace_execution_state(ClassicGroupExecutionState::Idle);
    let ClassicGroupExecutionState::PartitionCountDriverOwned { prepared, call } = state else {
        panic!("accepted count call expected");
    };
    let cycle = prepared.cycle();

    assert!(expire_count_cycle(entry, prepared, call, cycle, Moment::from_tick(3)).is_err());
    assert!(matches!(
        entry.execution.borrow_execution_state(),
        ClassicGroupExecutionState::PartitionCountCompletionFault { .. }
    ));
    assert!(matches!(
        entry.fault.as_ref(),
        Some(ClassicGroupEntryFault::PartitionCount(
            ClassicGroupPartitionCountFault::Semantic
        ))
    ));

    driver
        .shutdown_with_turn_limit(64, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("driver shutdown failed: {error}"));
    registry
        .recover_classic_calls_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("classic recovery failed: {error:?}"));
    super::registry_test_support::stop_registry(&mut registry);
}
