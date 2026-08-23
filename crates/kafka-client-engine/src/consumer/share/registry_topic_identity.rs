//! One bounded topic-identity action before initial share membership submission.

use std::sync::Arc;

use kafka_client_core::{ShareGroupHeartbeatFailure, TopicId};

use crate::{
    clock::OperationDeadline,
    driver::{
        DriverOwner, TopicPartitionCountAdmissionFailureKind, TopicPartitionCountCall,
        TopicPartitionCountFact, TopicPartitionCountFailure,
    },
};

use super::{
    ShareMembershipCatalog, ShareMembershipInterpreter,
    catalog::{ShareMembershipCatalogError, ShareTopicIdentity},
    entry::ShareConsumerEntry,
    registry::ShareConsumerRegistry,
    topic_identity_call::ShareTopicIdentityCall,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum ShareTopicIdentityTurn {
    Idle,
    Progress,
    Blocked,
}

impl ShareConsumerRegistry {
    pub(in crate::consumer) fn turn_one_topic_identity(
        &mut self,
        now: kafka_client_core::Moment,
        driver: &DriverOwner,
    ) -> Result<ShareTopicIdentityTurn, ShareTopicIdentityError> {
        if let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.topic_call.is_some())
        {
            return settle_topic_identity(&mut self.entries[index]);
        }
        let Some(index) = self.entries.iter().position(topic_identity_is_ready) else {
            return Ok(ShareTopicIdentityTurn::Idle);
        };
        let entry = &mut self.entries[index];
        let capture = entry.start.ok_or(ShareTopicIdentityError::EffectShape)?;
        if capture.deadline().is_elapsed_at(now) {
            terminalize(entry, ShareGroupHeartbeatFailure::DeadlineElapsed)?;
            return Ok(ShareTopicIdentityTurn::Progress);
        }
        let topic_index = entry.resolved_topics.len();
        let topic_id = entry
            .local_topic_id(topic_index)
            .ok_or(ShareTopicIdentityError::EffectShape)?;
        let topic = Arc::clone(
            entry
                .topics()
                .get(topic_index)
                .ok_or(ShareTopicIdentityError::EffectShape)?,
        );
        match TopicPartitionCountCall::submit(
            driver,
            &topic,
            capture.operation_deadline().transport(),
        ) {
            Ok(call) => {
                entry.topic_call = Some(ShareTopicIdentityCall {
                    local_topic_id: topic_id,
                    topic,
                    deadline: capture.operation_deadline(),
                    call,
                });
                Ok(ShareTopicIdentityTurn::Progress)
            }
            Err(error) if error.kind() == TopicPartitionCountAdmissionFailureKind::Full => {
                Ok(ShareTopicIdentityTurn::Blocked)
            }
            Err(_error) => {
                terminalize(entry, ShareGroupHeartbeatFailure::Execution)?;
                Ok(ShareTopicIdentityTurn::Progress)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum ShareTopicIdentityError {
    EffectShape,
    AlreadyTerminal,
}

fn topic_identity_is_ready(entry: &ShareConsumerEntry) -> bool {
    !entry.has_close()
        && entry.start.is_some()
        && entry.membership.is_none()
        && entry.fault.is_none()
        && entry.topic_call.is_none()
        && entry.resolved_topics.len() < entry.topics().len()
}

fn settle_topic_identity(
    entry: &mut ShareConsumerEntry,
) -> Result<ShareTopicIdentityTurn, ShareTopicIdentityError> {
    let mut call = entry
        .topic_call
        .take()
        .ok_or(ShareTopicIdentityError::EffectShape)?;
    let capture = entry.start.ok_or(ShareTopicIdentityError::EffectShape)?;
    let expected_index = entry.resolved_topics.len();
    let expected_id = entry.local_topic_id(expected_index);
    let expected_name = entry.topics().get(expected_index);
    if expected_id != Some(call.local_topic_id)
        || expected_name != Some(&call.topic)
        || capture.operation_deadline() != call.deadline
    {
        entry.topic_call = Some(call);
        return Err(ShareTopicIdentityError::EffectShape);
    }
    let Some(terminal) = call.call.try_terminal() else {
        entry.topic_call = Some(call);
        return Ok(ShareTopicIdentityTurn::Blocked);
    };
    match terminal {
        Ok(fact) => {
            complete_topic_identity(entry, call.local_topic_id, call.topic, call.deadline, fact)?;
        }
        Err(failure) => terminalize(entry, topic_lookup_failure(failure))?,
    }
    Ok(ShareTopicIdentityTurn::Progress)
}

pub(super) fn complete_topic_identity(
    entry: &mut ShareConsumerEntry,
    local_topic_id: TopicId,
    topic: Arc<str>,
    deadline: OperationDeadline,
    fact: TopicPartitionCountFact,
) -> Result<(), ShareTopicIdentityError> {
    let capture = entry.start.ok_or(ShareTopicIdentityError::EffectShape)?;
    let index = entry.resolved_topics.len();
    if entry.local_topic_id(index) != Some(local_topic_id)
        || entry.topics().get(index) != Some(&topic)
        || capture.operation_deadline() != deadline
    {
        return Err(ShareTopicIdentityError::EffectShape);
    }
    let TopicPartitionCountFact {
        metadata_generation: _,
        logical_partition_count,
        kafka_topic_id,
    } = fact;
    let Some(kafka_topic_id) = kafka_topic_id else {
        return terminalize(entry, ShareGroupHeartbeatFailure::InvalidResponse);
    };
    if logical_partition_count == 0 || kafka_topic_id == [0; 16] {
        return terminalize(entry, ShareGroupHeartbeatFailure::InvalidResponse);
    }
    entry.resolved_topics.push(ShareTopicIdentity::new(
        local_topic_id,
        topic,
        kafka_topic_id,
        logical_partition_count,
    ));
    if entry.resolved_topics.len() == entry.topics().len() {
        install_membership(entry)?;
    }
    Ok(())
}

fn install_membership(entry: &mut ShareConsumerEntry) -> Result<(), ShareTopicIdentityError> {
    let capture = entry.start.ok_or(ShareTopicIdentityError::EffectShape)?;
    let topics = std::mem::take(&mut entry.resolved_topics);
    let catalog = match ShareMembershipCatalog::try_new(
        Arc::clone(entry.group()),
        Arc::clone(entry.member()),
        entry.rack().cloned(),
        topics,
    ) {
        Ok(catalog) => catalog,
        Err(error) => return terminalize(entry, catalog_failure(error)),
    };
    let mut membership = ShareMembershipInterpreter::new(
        entry.group_id(),
        entry.member_id(),
        ShareConsumerEntry::policy(),
        catalog,
    );
    if membership.begin(capture).is_err() {
        return terminalize(entry, ShareGroupHeartbeatFailure::Execution);
    }
    entry.membership = Some(membership);
    Ok(())
}

fn terminalize(
    entry: &mut ShareConsumerEntry,
    failure: ShareGroupHeartbeatFailure,
) -> Result<(), ShareTopicIdentityError> {
    if entry.fault.is_some() {
        return Err(ShareTopicIdentityError::AlreadyTerminal);
    }
    entry.fault = Some(failure);
    drop(entry.topic_call.take());
    Ok(())
}

const fn catalog_failure(error: ShareMembershipCatalogError) -> ShareGroupHeartbeatFailure {
    match error {
        ShareMembershipCatalogError::Allocation => ShareGroupHeartbeatFailure::Execution,
        _ => ShareGroupHeartbeatFailure::InvalidResponse,
    }
}

pub(super) const fn topic_lookup_failure(
    failure: TopicPartitionCountFailure,
) -> ShareGroupHeartbeatFailure {
    match failure {
        TopicPartitionCountFailure::Deadline => ShareGroupHeartbeatFailure::DeadlineElapsed,
        TopicPartitionCountFailure::Malformed | TopicPartitionCountFailure::TopicMismatch => {
            ShareGroupHeartbeatFailure::InvalidResponse
        }
        TopicPartitionCountFailure::Broker(error_code) => {
            ShareGroupHeartbeatFailure::Broker(error_code)
        }
        TopicPartitionCountFailure::Unavailable
        | TopicPartitionCountFailure::Refresh
        | TopicPartitionCountFailure::Allocation
        | TopicPartitionCountFailure::QueryCapacity(_)
        | TopicPartitionCountFailure::Capacity { .. }
        | TopicPartitionCountFailure::Draining
        | TopicPartitionCountFailure::Completion
        | TopicPartitionCountFailure::UnrecognizedDriverFailure => {
            ShareGroupHeartbeatFailure::Execution
        }
    }
}
