//! Classification and retained ownership of partition-count mechanism failures.

use kafka_client_core::ClassicGroupInput;

use crate::driver::TopicPartitionCountFailure;

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_join::ClassicGroupExecutionState,
    classic_group_partition_count_call::ClassicGroupPartitionCountCall,
    classic_group_partition_count_settlement::ClassicGroupPartitionCountSettlementTurn,
    classic_group_partition_counts::PreparedClassicGroupPartitionCounts,
    registry_entry::GroupConsumerEntry,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupPartitionCountFailureDisposition {
    CycleFailed,
    DeadlineElapsed,
    Fault,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupPartitionCountFault {
    Identity,
    Completion(TopicPartitionCountFailure),
    Progress,
    Semantic,
    Materialization,
}

pub(super) const fn classify_partition_count_failure(
    failure: TopicPartitionCountFailure,
    deadline_elapsed: bool,
) -> ClassicGroupPartitionCountFailureDisposition {
    match failure {
        TopicPartitionCountFailure::TopicMismatch | TopicPartitionCountFailure::Completion => {
            ClassicGroupPartitionCountFailureDisposition::Fault
        }
        TopicPartitionCountFailure::Deadline
        | TopicPartitionCountFailure::Unavailable
        | TopicPartitionCountFailure::Refresh
        | TopicPartitionCountFailure::Broker(_)
        | TopicPartitionCountFailure::Malformed
        | TopicPartitionCountFailure::Allocation
        | TopicPartitionCountFailure::QueryCapacity(_)
        | TopicPartitionCountFailure::Capacity { .. }
        | TopicPartitionCountFailure::Draining
            if deadline_elapsed =>
        {
            ClassicGroupPartitionCountFailureDisposition::DeadlineElapsed
        }
        TopicPartitionCountFailure::Deadline
        | TopicPartitionCountFailure::Unavailable
        | TopicPartitionCountFailure::Refresh
        | TopicPartitionCountFailure::Broker(_)
        | TopicPartitionCountFailure::Malformed
        | TopicPartitionCountFailure::Allocation
        | TopicPartitionCountFailure::QueryCapacity(_)
        | TopicPartitionCountFailure::Capacity { .. }
        | TopicPartitionCountFailure::Draining => {
            ClassicGroupPartitionCountFailureDisposition::CycleFailed
        }
    }
}

pub(super) fn fail_count_cycle(
    entry: &mut GroupConsumerEntry,
    prepared: PreparedClassicGroupPartitionCounts,
    call: ClassicGroupPartitionCountCall,
    cycle: kafka_client_core::MembershipCycle,
) -> Result<ClassicGroupPartitionCountSettlementTurn, ClassicGroupExecutionError> {
    apply_count_terminal(
        entry,
        prepared,
        call,
        ClassicGroupInput::PartitionCountsFailed { cycle },
    )
}

pub(super) fn expire_count_cycle(
    entry: &mut GroupConsumerEntry,
    prepared: PreparedClassicGroupPartitionCounts,
    call: ClassicGroupPartitionCountCall,
    cycle: kafka_client_core::MembershipCycle,
    now: kafka_client_core::Moment,
) -> Result<ClassicGroupPartitionCountSettlementTurn, ClassicGroupExecutionError> {
    apply_count_terminal(
        entry,
        prepared,
        call,
        ClassicGroupInput::DeadlineElapsed { cycle, now },
    )
}

fn apply_count_terminal(
    entry: &mut GroupConsumerEntry,
    prepared: PreparedClassicGroupPartitionCounts,
    call: ClassicGroupPartitionCountCall,
    input: ClassicGroupInput,
) -> Result<ClassicGroupPartitionCountSettlementTurn, ClassicGroupExecutionError> {
    let transition = match entry.classic.apply(input) {
        Ok(transition) => transition,
        Err(error) => {
            return freeze_partition_count_call(
                entry,
                prepared,
                call,
                ClassicGroupPartitionCountFault::Semantic,
                ClassicGroupExecutionError::Core(error.kind()),
            );
        }
    };
    if transition.into_effects().next().is_some() {
        return freeze_partition_count_call(
            entry,
            prepared,
            call,
            ClassicGroupPartitionCountFault::Semantic,
            ClassicGroupExecutionError::PartitionCountTerminal,
        );
    }
    entry
        .execution
        .set_execution_state(ClassicGroupExecutionState::Idle);
    Ok(ClassicGroupPartitionCountSettlementTurn::Progress)
}

pub(super) fn freeze_partition_count_call(
    entry: &mut GroupConsumerEntry,
    prepared: PreparedClassicGroupPartitionCounts,
    call: ClassicGroupPartitionCountCall,
    fault: ClassicGroupPartitionCountFault,
    error: ClassicGroupExecutionError,
) -> Result<ClassicGroupPartitionCountSettlementTurn, ClassicGroupExecutionError> {
    entry.execution.set_execution_state(
        ClassicGroupExecutionState::PartitionCountCompletionFault { prepared, call },
    );
    entry.fault = Some(ClassicGroupEntryFault::PartitionCount(fault));
    Err(error)
}
