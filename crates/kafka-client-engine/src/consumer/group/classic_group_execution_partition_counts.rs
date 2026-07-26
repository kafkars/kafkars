//! Fenced scalar partition-count ingress for one prepared classic leader.

use kafka_client_core::Moment;

use super::{
    classic_group_execution::{ClassicGroupExecution, ClassicGroupExecutionError},
    classic_group_join::ClassicGroupExecutionState,
    classic_group_owner::ClassicGroupOwner,
    classic_group_owner_leader::ClassicGroupLeaderCountError,
    classic_group_partition_count_call::{
        ClassicGroupPartitionCountCall, ClassicGroupPartitionCountCallIdentity,
    },
    classic_group_partition_counts::PreparedClassicGroupPartitionCounts,
    session_catalog::GroupSessionCatalog,
};

impl ClassicGroupExecution {
    pub(super) const fn prepared_partition_counts(
        &self,
    ) -> Option<&PreparedClassicGroupPartitionCounts> {
        match self.borrow_execution_state() {
            ClassicGroupExecutionState::PreparedPartitionCounts(prepared) => Some(prepared),
            _ => None,
        }
    }

    pub(super) fn begin_partition_count_handoff(
        &mut self,
        group_id: kafka_client_core::GroupId,
        topic_id: kafka_client_core::TopicId,
    ) -> Result<ClassicGroupPartitionCountCallIdentity, ClassicGroupExecutionError> {
        let state = self.replace_execution_state(ClassicGroupExecutionState::Idle);
        let ClassicGroupExecutionState::PreparedPartitionCounts(prepared) = state else {
            self.set_execution_state(state);
            return Err(ClassicGroupExecutionError::PartitionCountsNotPrepared);
        };
        if prepared.next_topic() != Some(topic_id) {
            self.set_execution_state(ClassicGroupExecutionState::PreparedPartitionCounts(
                prepared,
            ));
            return Err(ClassicGroupExecutionError::PartitionCountFence);
        }
        let identity = ClassicGroupPartitionCountCallIdentity::new(
            group_id,
            prepared.cycle(),
            topic_id,
            prepared.deadline(),
        );
        self.set_execution_state(ClassicGroupExecutionState::PartitionCountHandoff {
            prepared,
            identity,
        });
        Ok(identity)
    }

    pub(super) fn restore_partition_count_handoff(
        &mut self,
        identity: ClassicGroupPartitionCountCallIdentity,
    ) -> Result<(), ClassicGroupExecutionError> {
        let state = self.replace_execution_state(ClassicGroupExecutionState::Idle);
        let ClassicGroupExecutionState::PartitionCountHandoff {
            prepared,
            identity: expected,
        } = state
        else {
            self.set_execution_state(state);
            return Err(ClassicGroupExecutionError::HandoffMismatch);
        };
        if expected != identity {
            self.set_execution_state(ClassicGroupExecutionState::PartitionCountHandoff {
                prepared,
                identity: expected,
            });
            return Err(ClassicGroupExecutionError::HandoffMismatch);
        }
        self.set_execution_state(ClassicGroupExecutionState::PreparedPartitionCounts(
            prepared,
        ));
        Ok(())
    }

    pub(super) fn confirm_partition_count_driver_owned(
        &mut self,
        identity: ClassicGroupPartitionCountCallIdentity,
        call: ClassicGroupPartitionCountCall,
    ) -> Result<(), ClassicGroupExecutionError> {
        let state = self.replace_execution_state(ClassicGroupExecutionState::Idle);
        let ClassicGroupExecutionState::PartitionCountHandoff {
            prepared,
            identity: expected,
        } = state
        else {
            self.set_execution_state(state);
            return Err(ClassicGroupExecutionError::HandoffMismatch);
        };
        if expected != identity || call.identity() != identity {
            self.set_execution_state(ClassicGroupExecutionState::PartitionCountDriverOwned {
                prepared,
                call,
            });
            return Err(ClassicGroupExecutionError::HandoffMismatch);
        }
        self.set_execution_state(ClassicGroupExecutionState::PartitionCountDriverOwned {
            prepared,
            call,
        });
        Ok(())
    }

    pub(super) fn complete_partition_counts(
        &mut self,
        owner: &mut ClassicGroupOwner,
        catalog: &GroupSessionCatalog,
        now: Moment,
    ) -> Result<(), ClassicGroupExecutionError> {
        let state = self.replace_execution_state(ClassicGroupExecutionState::Idle);
        let ClassicGroupExecutionState::PreparedPartitionCounts(prepared) = state else {
            self.set_execution_state(state);
            return Err(ClassicGroupExecutionError::PartitionCountsNotPrepared);
        };
        match owner.apply_leader_partition_counts(catalog, &prepared, now) {
            Ok(sync) => {
                self.set_execution_state(ClassicGroupExecutionState::PreparedSync(sync));
                Ok(())
            }
            Err(
                error @ (ClassicGroupLeaderCountError::UnexpectedSyncEffect
                | ClassicGroupLeaderCountError::SyncRequest(_)),
            ) => {
                self.set_execution_state(ClassicGroupExecutionState::PartitionCountsPostCore {
                    _retained_partition_counts: prepared,
                });
                let _ = error;
                Err(ClassicGroupExecutionError::LeaderPartitionCounts)
            }
            Err(error) => {
                self.set_execution_state(ClassicGroupExecutionState::PreparedPartitionCounts(
                    prepared,
                ));
                let _ = error;
                Err(ClassicGroupExecutionError::LeaderPartitionCounts)
            }
        }
    }

    pub(super) fn fail_prepared_partition_counts(
        &mut self,
        owner: &mut ClassicGroupOwner,
    ) -> Result<(), ClassicGroupExecutionError> {
        let state = self.replace_execution_state(ClassicGroupExecutionState::Idle);
        let ClassicGroupExecutionState::PreparedPartitionCounts(prepared) = state else {
            self.set_execution_state(state);
            return Err(ClassicGroupExecutionError::PartitionCountsNotPrepared);
        };
        let cycle = prepared.cycle();
        let transition = match owner
            .apply(kafka_client_core::ClassicGroupInput::PartitionCountsFailed { cycle })
        {
            Ok(transition) => transition,
            Err(error) => {
                self.set_execution_state(ClassicGroupExecutionState::PreparedPartitionCounts(
                    prepared,
                ));
                return Err(ClassicGroupExecutionError::Core(error.kind()));
            }
        };
        if transition.into_effects().next().is_some() {
            self.set_execution_state(ClassicGroupExecutionState::PreparedPartitionCounts(
                prepared,
            ));
            return Err(ClassicGroupExecutionError::PartitionCountTerminal);
        }
        Ok(())
    }
}
