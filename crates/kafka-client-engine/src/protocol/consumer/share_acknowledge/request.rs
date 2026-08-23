//! Canonical explicit-only `ShareAcknowledge` v1 request materialization.

use kafka_client_core::{ShareAcknowledgeAttempt, ShareAcknowledgement, ShareAcknowledgementBatch};
use kafka_wire::{
    ShareAcknowledgeRequest,
    share_acknowledge_request::{AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch},
};
use kafka_wire_core::Uuid;

use super::{
    ShareAcknowledgeCorrelation, ShareAcknowledgeRequestFailure,
    model::{
        SHARE_ACKNOWLEDGE_MAX_BATCHES, SHARE_ACKNOWLEDGE_MAX_PARTITIONS,
        SHARE_ACKNOWLEDGE_MAX_TOPICS, ShareAcknowledgePartitionKey,
    },
};

const MAX_KAFKA_STRING_BYTES: usize = i16::MAX as usize;

/// Linear generated request and its complete response correlation.
#[must_use = "a prepared ShareAcknowledge request must be submitted or released"]
pub(crate) struct PreparedShareAcknowledgeRequest {
    request: ShareAcknowledgeRequest,
    correlation: ShareAcknowledgeCorrelation,
}

impl PreparedShareAcknowledgeRequest {
    pub(crate) fn into_parts(self) -> (ShareAcknowledgeRequest, ShareAcknowledgeCorrelation) {
        (self.request, self.correlation)
    }

    #[cfg(test)]
    pub(super) const fn request_for_test(&self) -> &ShareAcknowledgeRequest {
        &self.request
    }
}

pub(crate) fn share_acknowledge_request(
    group_id: &str,
    member_id: &str,
    attempt: ShareAcknowledgeAttempt,
    acknowledgement: &ShareAcknowledgement,
) -> Result<PreparedShareAcknowledgeRequest, ShareAcknowledgeRequestFailure> {
    validate_identity(group_id, member_id)?;
    validate_fence(attempt, acknowledgement)?;
    let (topics, correlation) = materialize(acknowledgement.batches())?;
    let mut request = ShareAcknowledgeRequest::default();
    request.group_id = Some(group_id.into());
    request.member_id = Some(member_id.into());
    request.share_session_epoch = attempt.fence().session_epoch().get();
    request.is_renew_ack = false;
    request.topics = topics;
    Ok(PreparedShareAcknowledgeRequest {
        request,
        correlation: ShareAcknowledgeCorrelation::new(correlation),
    })
}

fn validate_identity(
    group_id: &str,
    member_id: &str,
) -> Result<(), ShareAcknowledgeRequestFailure> {
    if group_id.is_empty() || group_id.len() > MAX_KAFKA_STRING_BYTES {
        return Err(ShareAcknowledgeRequestFailure::GroupId);
    }
    if member_id.is_empty() || member_id.len() > MAX_KAFKA_STRING_BYTES {
        return Err(ShareAcknowledgeRequestFailure::MemberId);
    }
    Ok(())
}

fn validate_fence(
    attempt: ShareAcknowledgeAttempt,
    acknowledgement: &ShareAcknowledgement,
) -> Result<(), ShareAcknowledgeRequestFailure> {
    let current = attempt.fence();
    let acquired = attempt.acquisition_fence();
    let next_epoch = acquired.session_epoch().get().checked_add(1);
    if current.session_epoch().get() <= 0
        || acknowledgement.fence() != acquired
        || current.broker_id() != acquired.broker_id()
        || current.group_id() != acquired.group_id()
        || current.member_id() != acquired.member_id()
        || current.member_epoch() != acquired.member_epoch()
        || next_epoch != Some(current.session_epoch().get())
    {
        return Err(ShareAcknowledgeRequestFailure::SessionFence);
    }
    Ok(())
}

fn materialize(
    source: &[ShareAcknowledgementBatch],
) -> Result<
    (Vec<AcknowledgeTopic>, Vec<ShareAcknowledgePartitionKey>),
    ShareAcknowledgeRequestFailure,
> {
    if source.is_empty() {
        return Err(ShareAcknowledgeRequestFailure::Empty);
    }
    let mut topics = Vec::new();
    let mut correlation = Vec::new();
    topics
        .try_reserve_exact(source.len().min(SHARE_ACKNOWLEDGE_MAX_TOPICS))
        .map_err(|_| ShareAcknowledgeRequestFailure::Allocation)?;
    correlation
        .try_reserve_exact(source.len().min(SHARE_ACKNOWLEDGE_MAX_PARTITIONS))
        .map_err(|_| ShareAcknowledgeRequestFailure::Allocation)?;
    let mut index = 0;
    while index < source.len() {
        let topic_id = source[index].topic_uuid().bytes();
        validate_topic_order(source, index, topic_id)?;
        let mut topic = AcknowledgeTopic::default();
        topic.topic_id = Uuid::from_bytes(topic_id);
        while index < source.len() && source[index].topic_uuid().bytes() == topic_id {
            let partition = source[index].partition().partition().get();
            let partition_index = i32::try_from(partition)
                .map_err(|_| ShareAcknowledgeRequestFailure::PartitionOutOfRange(partition))?;
            correlation.push(ShareAcknowledgePartitionKey {
                topic_id,
                partition,
            });
            let mut generated = AcknowledgePartition::default();
            generated.partition_index = partition_index;
            while index < source.len()
                && source[index].topic_uuid().bytes() == topic_id
                && source[index].partition().partition().get() == partition
            {
                generated
                    .acknowledgement_batches
                    .try_reserve(1)
                    .map_err(|_| ShareAcknowledgeRequestFailure::Allocation)?;
                generated
                    .acknowledgement_batches
                    .push(materialize_batch(&source[index])?);
                index += 1;
            }
            topic
                .partitions
                .try_reserve(1)
                .map_err(|_| ShareAcknowledgeRequestFailure::Allocation)?;
            topic.partitions.push(generated);
        }
        topics.push(topic);
    }
    validate_counts(&topics, &correlation, source.len())?;
    Ok((topics, correlation))
}

fn materialize_batch(
    source: &ShareAcknowledgementBatch,
) -> Result<AcknowledgementBatch, ShareAcknowledgeRequestFailure> {
    let count = source
        .last_offset()
        .checked_sub(source.first_offset())
        .and_then(|difference| difference.checked_add(1))
        .and_then(|count| usize::try_from(count).ok())
        .ok_or(ShareAcknowledgeRequestFailure::InvalidOffsets {
            first: source.first_offset(),
            last: source.last_offset(),
        })?;
    if source.first_offset() < 0
        || !(source.acknowledge_types().len() == 1 || source.acknowledge_types().len() == count)
    {
        return Err(ShareAcknowledgeRequestFailure::InvalidAcknowledgeTypes);
    }
    let mut acknowledge_types = Vec::new();
    acknowledge_types
        .try_reserve_exact(source.acknowledge_types().len())
        .map_err(|_| ShareAcknowledgeRequestFailure::Allocation)?;
    acknowledge_types.extend(
        source
            .acknowledge_types()
            .iter()
            .map(|value| value.wire_value()),
    );
    let mut batch = AcknowledgementBatch::default();
    batch.first_offset = source.first_offset();
    batch.last_offset = source.last_offset();
    batch.acknowledge_types = acknowledge_types;
    Ok(batch)
}

fn validate_topic_order(
    source: &[ShareAcknowledgementBatch],
    index: usize,
    topic_id: [u8; 16],
) -> Result<(), ShareAcknowledgeRequestFailure> {
    if topic_id == [0; 16] {
        return Err(ShareAcknowledgeRequestFailure::ZeroTopicId);
    }
    if index > 0 && source[index - 1].topic_uuid().bytes() >= topic_id {
        return Err(ShareAcknowledgeRequestFailure::NoncanonicalOrder);
    }
    Ok(())
}

fn validate_counts(
    topics: &[AcknowledgeTopic],
    partitions: &[ShareAcknowledgePartitionKey],
    batches: usize,
) -> Result<(), ShareAcknowledgeRequestFailure> {
    for (actual, limit, failure) in [
        (topics.len(), SHARE_ACKNOWLEDGE_MAX_TOPICS, 0_u8),
        (partitions.len(), SHARE_ACKNOWLEDGE_MAX_PARTITIONS, 1),
        (batches, SHARE_ACKNOWLEDGE_MAX_BATCHES, 2),
    ] {
        if actual > limit {
            return Err(match failure {
                0 => ShareAcknowledgeRequestFailure::TopicCount { actual, limit },
                1 => ShareAcknowledgeRequestFailure::PartitionCount { actual, limit },
                _ => ShareAcknowledgeRequestFailure::BatchCount { actual, limit },
            });
        }
    }
    if partitions.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ShareAcknowledgeRequestFailure::NoncanonicalOrder);
    }
    Ok(())
}
