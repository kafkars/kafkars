//! Post-driver recovery of one exact accepted partition-count call.

use kafka_client_core::ClassicGroupInput;

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::{ClassicGroupExecution, ClassicGroupExecutionError},
    classic_group_join::ClassicGroupExecutionState,
    classic_group_owner::ClassicGroupOwner,
    classic_group_partition_count_failure::ClassicGroupPartitionCountFault,
    registry::GroupConsumerRegistry,
    session_catalog::GroupSessionCatalog,
};

impl ClassicGroupExecution {
    #[expect(
        clippy::maybe_infinite_iter,
        reason = "the flagged cycle calls return fixed scalar fences and perform no iteration"
    )]
    pub(super) fn recover_partition_count_after_driver_shutdown(
        &mut self,
        owner: &mut ClassicGroupOwner,
        catalog: &GroupSessionCatalog,
    ) -> Result<bool, ClassicGroupExecutionError> {
        let state = self.replace_execution_state(ClassicGroupExecutionState::Idle);
        let (prepared, call) = match state {
            ClassicGroupExecutionState::PartitionCountDriverOwned { prepared, call }
            | ClassicGroupExecutionState::PartitionCountCompletionFault { prepared, call } => {
                (prepared, call)
            }
            state => {
                self.set_execution_state(state);
                return Ok(false);
            }
        };
        let identity = call.identity();
        let topic_matches = owner
            .pending()
            .and_then(|candidate| candidate.topic_name(catalog, identity.topic_id()))
            .is_some_and(|topic| topic.as_ref() == call.topic().as_ref());
        if identity.group_id() != owner.machine().group_id()
            || identity.cycle() != prepared.cycle()
            || identity.deadline() != prepared.deadline()
            || prepared.next_topic() != Some(identity.topic_id())
            || !topic_matches
        {
            self.set_execution_state(ClassicGroupExecutionState::PartitionCountCompletionFault {
                prepared,
                call,
            });
            return Err(ClassicGroupExecutionError::HandoffMismatch);
        }
        call.discard_after_driver_shutdown();
        let transition = match owner.apply(ClassicGroupInput::PartitionCountsFailed {
            cycle: identity.cycle(),
        }) {
            Ok(transition) => transition,
            Err(error) => {
                self.set_execution_state(ClassicGroupExecutionState::PartitionCountsPostCore {
                    _retained_partition_counts: prepared,
                });
                return Err(ClassicGroupExecutionError::Core(error.kind()));
            }
        };
        if transition.into_effects().next().is_some() {
            self.set_execution_state(ClassicGroupExecutionState::PartitionCountsPostCore {
                _retained_partition_counts: prepared,
            });
            return Err(ClassicGroupExecutionError::PartitionCountTerminal);
        }
        Ok(true)
    }
}

impl GroupConsumerRegistry {
    pub(super) fn recover_classic_partition_counts_after_driver_shutdown(
        &mut self,
    ) -> Result<(), ClassicGroupExecutionError> {
        for entry in &mut self.entries {
            let state = entry.execution.borrow_execution_state();
            let count_owned = matches!(
                state,
                ClassicGroupExecutionState::PartitionCountDriverOwned { .. }
                    | ClassicGroupExecutionState::PartitionCountCompletionFault { .. }
            );
            if !count_owned {
                continue;
            }
            if entry
                .fault
                .as_ref()
                .is_some_and(|fault| !matches!(fault, ClassicGroupEntryFault::PartitionCount(_)))
            {
                return Err(ClassicGroupExecutionError::EntryFault);
            }
            match entry
                .execution
                .recover_partition_count_after_driver_shutdown(&mut entry.classic, &entry.catalog)
            {
                Ok(true) => {
                    if matches!(
                        entry.fault.as_ref(),
                        Some(ClassicGroupEntryFault::PartitionCount(_))
                    ) {
                        drop(entry.fault.take());
                    }
                }
                Ok(false) => {}
                Err(error) => {
                    entry.fault = Some(ClassicGroupEntryFault::PartitionCount(
                        ClassicGroupPartitionCountFault::Semantic,
                    ));
                    return Err(error);
                }
            }
        }
        Ok(())
    }
}
