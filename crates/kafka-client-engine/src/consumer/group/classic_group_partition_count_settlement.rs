//! One bounded partition-count terminal interpretation per membership turn.

use crate::driver::TopicPartitionCountFact;

use super::{
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_join::ClassicGroupExecutionState,
    classic_group_partition_count_failure::{
        ClassicGroupPartitionCountFailureDisposition, ClassicGroupPartitionCountFault,
        classify_partition_count_failure, expire_count_cycle, fail_count_cycle,
        freeze_partition_count_call,
    },
    classic_group_partition_counts::ClassicGroupPartitionCountProgress,
    registry::GroupConsumerRegistry,
    registry_entry::GroupConsumerEntry,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupPartitionCountSettlementTurn {
    Idle,
    Progress,
}

impl GroupConsumerRegistry {
    pub(super) fn settle_one_classic_partition_count(
        &mut self,
        now: kafka_client_core::Moment,
    ) -> Result<ClassicGroupPartitionCountSettlementTurn, ClassicGroupExecutionError> {
        if let Some(index) = self.entries.iter().position(|entry| {
            entry.is_active()
                && entry
                    .execution
                    .prepared_partition_counts()
                    .is_some_and(|prepared| {
                        prepared.is_complete() && !prepared.deadline().core().is_elapsed_at(now)
                    })
        }) {
            return complete_entry_counts(&mut self.entries[index], now);
        }
        let Some(index) = self.entries.iter().position(|entry| {
            let state = entry.execution.borrow_execution_state();
            matches!(
                state,
                ClassicGroupExecutionState::PartitionCountDriverOwned { .. }
            )
        }) else {
            return Ok(ClassicGroupPartitionCountSettlementTurn::Idle);
        };
        settle_entry(&mut self.entries[index], now)
    }
}

#[expect(
    clippy::maybe_infinite_iter,
    reason = "the flagged cycle calls return fixed scalar fences and perform no iteration"
)]
fn settle_entry(
    entry: &mut GroupConsumerEntry,
    now: kafka_client_core::Moment,
) -> Result<ClassicGroupPartitionCountSettlementTurn, ClassicGroupExecutionError> {
    let state = entry
        .execution
        .replace_execution_state(ClassicGroupExecutionState::Idle);
    let ClassicGroupExecutionState::PartitionCountDriverOwned { prepared, mut call } = state else {
        entry.execution.set_execution_state(state);
        return Err(ClassicGroupExecutionError::PartitionCountsNotPrepared);
    };
    let identity = call.identity();
    let topic_matches = entry
        .classic
        .pending()
        .and_then(|candidate| candidate.topic_name(&entry.catalog, identity.topic_id()))
        .is_some_and(|topic| topic.as_ref() == call.topic().as_ref());
    if identity.group_id() != entry.group_id()
        || identity.cycle() != prepared.cycle()
        || identity.deadline() != prepared.deadline()
        || prepared.next_topic() != Some(identity.topic_id())
        || !topic_matches
    {
        return freeze_partition_count_call(
            entry,
            prepared,
            call,
            ClassicGroupPartitionCountFault::Identity,
            ClassicGroupExecutionError::HandoffMismatch,
        );
    }
    let Some(terminal) = call.try_terminal() else {
        entry.execution.set_execution_state(
            ClassicGroupExecutionState::PartitionCountDriverOwned { prepared, call },
        );
        return Ok(ClassicGroupPartitionCountSettlementTurn::Idle);
    };
    match terminal {
        Ok(fact) => settle_count_fact(entry, prepared, call, identity, fact, now),
        Err(failure)
            if classify_partition_count_failure(
                failure,
                prepared.deadline().core().is_elapsed_at(now),
            ) == ClassicGroupPartitionCountFailureDisposition::Fault =>
        {
            freeze_partition_count_call(
                entry,
                prepared,
                call,
                ClassicGroupPartitionCountFault::Completion(failure),
                ClassicGroupExecutionError::PartitionCountTerminal,
            )
        }
        Err(failure)
            if classify_partition_count_failure(
                failure,
                prepared.deadline().core().is_elapsed_at(now),
            ) == ClassicGroupPartitionCountFailureDisposition::DeadlineElapsed =>
        {
            expire_count_cycle(entry, prepared, call, identity.cycle(), now)
        }
        Err(_failure) => fail_count_cycle(entry, prepared, call, identity.cycle()),
    }
}

pub(super) fn settle_count_fact(
    entry: &mut GroupConsumerEntry,
    mut prepared: super::classic_group_partition_counts::PreparedClassicGroupPartitionCounts,
    call: super::classic_group_partition_count_call::ClassicGroupPartitionCountCall,
    identity: super::classic_group_partition_count_call::ClassicGroupPartitionCountCallIdentity,
    fact: TopicPartitionCountFact,
    now: kafka_client_core::Moment,
) -> Result<ClassicGroupPartitionCountSettlementTurn, ClassicGroupExecutionError> {
    if prepared.deadline().core().is_elapsed_at(now) {
        return expire_count_cycle(entry, prepared, call, identity.cycle(), now);
    }
    if !entry.is_active() {
        return fail_count_cycle(entry, prepared, call, identity.cycle());
    }
    let progress = match prepared.append(
        identity.topic_id(),
        fact.logical_partition_count,
        fact.metadata_generation,
    ) {
        Ok(progress) => progress,
        Err(_error) => {
            return freeze_partition_count_call(
                entry,
                prepared,
                call,
                ClassicGroupPartitionCountFault::Progress,
                ClassicGroupExecutionError::PartitionCountFence,
            );
        }
    };
    if progress == ClassicGroupPartitionCountProgress::Restarted || !prepared.is_complete() {
        entry
            .execution
            .set_execution_state(ClassicGroupExecutionState::PreparedPartitionCounts(
                prepared,
            ));
        return Ok(ClassicGroupPartitionCountSettlementTurn::Progress);
    }
    entry
        .execution
        .set_execution_state(ClassicGroupExecutionState::PreparedPartitionCounts(
            prepared,
        ));
    complete_entry_counts(entry, now)
}

fn complete_entry_counts(
    entry: &mut GroupConsumerEntry,
    now: kafka_client_core::Moment,
) -> Result<ClassicGroupPartitionCountSettlementTurn, ClassicGroupExecutionError> {
    match entry
        .execution
        .complete_partition_counts(&mut entry.classic, &entry.catalog, now)
    {
        Ok(()) => Ok(ClassicGroupPartitionCountSettlementTurn::Progress),
        Err(error) => {
            entry.fault = Some(
                super::classic_group_entry_fault::ClassicGroupEntryFault::PartitionCount(
                    ClassicGroupPartitionCountFault::Materialization,
                ),
            );
            Err(error)
        }
    }
}
