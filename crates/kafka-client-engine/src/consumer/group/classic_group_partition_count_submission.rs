//! One exact ordered topic-view handoff from a prepared classic leader.

use std::sync::Arc;

use crate::driver::{DriverOwner, TopicPartitionCountAdmissionFailureKind};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_execution::ClassicGroupExecutionError,
    classic_group_partition_count_call::ClassicGroupPartitionCountCall,
    classic_group_partition_count_failure::ClassicGroupPartitionCountFault,
    registry::GroupConsumerRegistry,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupPartitionCountSubmissionTurn {
    Idle,
    Progress,
    Blocked,
}

impl GroupConsumerRegistry {
    pub(super) fn submit_one_classic_partition_count(
        &mut self,
        driver: &DriverOwner,
    ) -> Result<ClassicGroupPartitionCountSubmissionTurn, ClassicGroupExecutionError> {
        let Some(index) = self.entries.iter().position(|entry| {
            entry.is_active()
                && entry
                    .execution
                    .prepared_partition_counts()
                    .is_some_and(|prepared| prepared.next_topic().is_some())
        }) else {
            return Ok(ClassicGroupPartitionCountSubmissionTurn::Idle);
        };
        let entry = &self.entries[index];
        let prepared = entry
            .execution
            .prepared_partition_counts()
            .ok_or(ClassicGroupExecutionError::PartitionCountsNotPrepared)?;
        let topic_id = prepared
            .next_topic()
            .ok_or(ClassicGroupExecutionError::PartitionCountFence)?;
        let candidate = entry
            .classic
            .pending()
            .ok_or(ClassicGroupExecutionError::PartitionCountFence)?;
        let topic = candidate
            .topic_name(&entry.catalog, topic_id)
            .map(Arc::clone)
            .ok_or(ClassicGroupExecutionError::PartitionCountFence)?;
        let entry = &mut self.entries[index];
        let identity = entry
            .execution
            .begin_partition_count_handoff(entry.group_id(), topic_id)?;
        match ClassicGroupPartitionCountCall::submit(driver, identity, topic) {
            Ok(call) => {
                entry
                    .execution
                    .confirm_partition_count_driver_owned(identity, call)?;
                Ok(ClassicGroupPartitionCountSubmissionTurn::Progress)
            }
            Err(failure) => {
                entry.execution.restore_partition_count_handoff(identity)?;
                match failure.kind() {
                    TopicPartitionCountAdmissionFailureKind::Full => {
                        Ok(ClassicGroupPartitionCountSubmissionTurn::Blocked)
                    }
                    TopicPartitionCountAdmissionFailureKind::Terminal => {
                        match entry
                            .execution
                            .fail_prepared_partition_counts(&mut entry.classic)
                        {
                            Ok(()) => Ok(ClassicGroupPartitionCountSubmissionTurn::Progress),
                            Err(error) => {
                                entry.fault = Some(ClassicGroupEntryFault::PartitionCount(
                                    ClassicGroupPartitionCountFault::Semantic,
                                ));
                                Err(error)
                            }
                        }
                    }
                }
            }
        }
    }
}
