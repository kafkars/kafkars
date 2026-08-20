//! One bounded metadata settlement or submission before initial API 68 execution.

use std::sync::Arc;

use kafka_client_core::{ConsumerGroupHeartbeatFailure, ConsumerGroupHeartbeatRequestKind, Moment};

use crate::driver::{
    DriverOwner, TopicPartitionCountAdmissionFailureKind, TopicPartitionCountFailure,
};

use super::{
    consumer_group_execution::ConsumerGroupExecutionError,
    consumer_group_execution_terminal::fail_consumer_group_entry,
    consumer_group_topic_identity_call::ConsumerGroupTopicIdentityCall,
    registry::GroupConsumerRegistry, registry_entry::GroupConsumerEntry,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ConsumerGroupTopicIdentityTurn {
    Idle,
    Progress,
    Blocked,
}

impl GroupConsumerRegistry {
    pub(super) fn turn_one_consumer_group_topic_identity(
        &mut self,
        now: Moment,
        driver: &DriverOwner,
    ) -> Result<ConsumerGroupTopicIdentityTurn, ConsumerGroupExecutionError> {
        if let Some(index) = self.entries.iter().position(|entry| {
            entry
                .consumer
                .as_ref()
                .is_some_and(|execution| execution.topic_identity_call().is_some())
        }) {
            return settle_topic_identity(&mut self.entries[index]);
        }
        let Some(index) = self
            .entries
            .iter()
            .position(consumer_topic_identity_is_ready)
        else {
            return Ok(ConsumerGroupTopicIdentityTurn::Idle);
        };
        let entry = &self.entries[index];
        let execution = entry
            .consumer
            .as_ref()
            .ok_or(ConsumerGroupExecutionError::MissingPrepared)?;
        let prepared = execution
            .prepared()
            .ok_or(ConsumerGroupExecutionError::MissingPrepared)?;
        if prepared.deadline().core().is_elapsed_at(now) {
            fail_consumer_group_entry(
                &mut self.entries[index],
                ConsumerGroupHeartbeatFailure::DeadlineElapsed,
            )?;
            return Ok(ConsumerGroupTopicIdentityTurn::Progress);
        }
        let topic_id = execution
            .topic_identities()
            .next_topic(entry.catalog.local_subscription())
            .ok_or(ConsumerGroupExecutionError::EffectShape)?;
        let topic = Arc::clone(
            entry
                .catalog
                .topic_name(topic_id)
                .map_err(|_error| ConsumerGroupExecutionError::EffectShape)?,
        );
        match ConsumerGroupTopicIdentityCall::submit(driver, topic_id, topic, prepared.deadline()) {
            Ok(call) => {
                self.entries[index]
                    .consumer
                    .as_mut()
                    .ok_or(ConsumerGroupExecutionError::MissingPrepared)?
                    .install_topic_identity_call(call)?;
                Ok(ConsumerGroupTopicIdentityTurn::Progress)
            }
            Err(error) if error.kind() == TopicPartitionCountAdmissionFailureKind::Full => {
                Ok(ConsumerGroupTopicIdentityTurn::Blocked)
            }
            Err(_error) => {
                fail_consumer_group_entry(
                    &mut self.entries[index],
                    ConsumerGroupHeartbeatFailure::Execution,
                )?;
                Ok(ConsumerGroupTopicIdentityTurn::Progress)
            }
        }
    }
}

pub(super) fn consumer_topic_identity_is_ready(entry: &GroupConsumerEntry) -> bool {
    entry.is_active()
        && entry.fault.is_none()
        && entry.consumer.as_ref().is_some_and(|execution| {
            execution
                .prepared()
                .is_some_and(|prepared| prepared.kind() == ConsumerGroupHeartbeatRequestKind::Join)
                && execution.topic_identity_call().is_none()
                && !execution.topic_identities().is_complete()
        })
}

fn settle_topic_identity(
    entry: &mut GroupConsumerEntry,
) -> Result<ConsumerGroupTopicIdentityTurn, ConsumerGroupExecutionError> {
    let execution = entry
        .consumer
        .as_mut()
        .ok_or(ConsumerGroupExecutionError::MissingPrepared)?;
    let mut call = execution.take_topic_identity_call()?;
    let expected_topic = execution
        .topic_identities()
        .next_topic(entry.catalog.local_subscription());
    let name_matches = entry
        .catalog
        .topic_name(call.topic_id())
        .is_ok_and(|topic| topic.as_ref() == call.topic().as_ref());
    let deadline_matches = execution
        .prepared()
        .is_some_and(|prepared| prepared.deadline() == call.deadline());
    if expected_topic != Some(call.topic_id()) || !name_matches || !deadline_matches {
        execution.restore_topic_identity_call(call)?;
        return Err(ConsumerGroupExecutionError::EffectShape);
    }
    let Some(terminal) = call.try_terminal() else {
        execution.restore_topic_identity_call(call)?;
        return Ok(ConsumerGroupTopicIdentityTurn::Blocked);
    };
    match terminal {
        Ok(fact) => {
            execution
                .topic_identities_mut()
                .append(call.topic_id(), fact)
                .map_err(|_error| ConsumerGroupExecutionError::EffectShape)?;
        }
        Err(failure) => fail_consumer_group_entry(entry, topic_lookup_failure(failure))?,
    }
    Ok(ConsumerGroupTopicIdentityTurn::Progress)
}

pub(super) const fn topic_lookup_failure(
    failure: TopicPartitionCountFailure,
) -> ConsumerGroupHeartbeatFailure {
    match failure {
        TopicPartitionCountFailure::Deadline => ConsumerGroupHeartbeatFailure::DeadlineElapsed,
        TopicPartitionCountFailure::Malformed | TopicPartitionCountFailure::TopicMismatch => {
            ConsumerGroupHeartbeatFailure::InvalidResponse
        }
        TopicPartitionCountFailure::Broker(error_code) => {
            ConsumerGroupHeartbeatFailure::Broker(error_code)
        }
        TopicPartitionCountFailure::Unavailable
        | TopicPartitionCountFailure::Refresh
        | TopicPartitionCountFailure::Allocation
        | TopicPartitionCountFailure::QueryCapacity(_)
        | TopicPartitionCountFailure::Capacity { .. }
        | TopicPartitionCountFailure::Draining
        | TopicPartitionCountFailure::Completion
        | TopicPartitionCountFailure::UnrecognizedDriverFailure => {
            ConsumerGroupHeartbeatFailure::Execution
        }
    }
}
