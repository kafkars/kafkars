//! Canonical, fetch-only `ShareFetch` v1 request materialization.

use kafka_wire::{
    ShareFetchRequest,
    share_fetch_request::{FetchPartition, FetchTopic, ForgottenTopic},
};
use kafka_wire_core::Uuid;

use super::{
    ShareFetchCorrelation, ShareFetchRequestFailure, ShareFetchRequestPlan, ShareFetchRequestTopic,
};

const MAX_KAFKA_STRING_BYTES: usize = i16::MAX as usize;

/// Positive or zero KIP-74 and acquisition bounds for one request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ShareFetchRequestSettings {
    pub(crate) max_wait_ms: u32,
    pub(crate) min_bytes: u32,
    pub(crate) max_bytes: u32,
    pub(crate) max_records: u32,
    pub(crate) batch_size: u32,
}

/// Linear generated request and its complete response correlation.
#[must_use = "a prepared ShareFetch request must be submitted or released"]
pub(crate) struct PreparedShareFetchRequest {
    request: ShareFetchRequest,
    correlation: ShareFetchCorrelation,
}

impl PreparedShareFetchRequest {
    pub(crate) fn into_parts(self) -> (ShareFetchRequest, ShareFetchCorrelation) {
        (self.request, self.correlation)
    }

    #[cfg(test)]
    pub(super) const fn request_for_test(&self) -> &ShareFetchRequest {
        &self.request
    }
}

pub(crate) fn share_fetch_request(
    group_id: &str,
    member_id: &str,
    session_epoch: i32,
    settings: ShareFetchRequestSettings,
    plan: ShareFetchRequestPlan,
) -> Result<PreparedShareFetchRequest, ShareFetchRequestFailure> {
    validate_scalar_request(group_id, member_id, session_epoch, settings)?;
    if session_epoch == 0 && !plan.is_complete_initial() {
        return Err(ShareFetchRequestFailure::InitialRequestShape);
    }
    let (active, included, forgotten) = plan.into_parts();
    let correlation = ShareFetchCorrelation::new(active);
    let mut request = ShareFetchRequest::default();
    request.group_id = Some(group_id.into());
    request.member_id = Some(member_id.into());
    request.share_session_epoch = session_epoch;
    request.max_wait_ms = to_i32(
        settings.max_wait_ms,
        ShareFetchRequestFailure::MaxWaitOutOfRange,
    )?;
    request.min_bytes = to_i32(
        settings.min_bytes,
        ShareFetchRequestFailure::MinBytesOutOfRange,
    )?;
    request.max_bytes = to_positive(
        settings.max_bytes,
        ShareFetchRequestFailure::MaxBytesOutOfRange,
    )?;
    request.max_records = to_positive(
        settings.max_records,
        ShareFetchRequestFailure::MaxRecordsOutOfRange,
    )?;
    request.batch_size = to_positive(
        settings.batch_size,
        ShareFetchRequestFailure::BatchSizeOutOfRange,
    )?;
    request.topics = materialize_topics(included)?;
    request.forgotten_topics_data = materialize_forgotten(forgotten)?;
    Ok(PreparedShareFetchRequest {
        request,
        correlation,
    })
}

pub(crate) fn share_fetch_close_request(
    group_id: &str,
    member_id: &str,
) -> Result<PreparedShareFetchRequest, ShareFetchRequestFailure> {
    validate_identity(group_id, member_id)?;
    let mut request = ShareFetchRequest::default();
    request.group_id = Some(group_id.into());
    request.member_id = Some(member_id.into());
    request.share_session_epoch = -1;
    Ok(PreparedShareFetchRequest {
        request,
        correlation: ShareFetchCorrelation::new(Vec::new()),
    })
}

fn validate_scalar_request(
    group_id: &str,
    member_id: &str,
    session_epoch: i32,
    settings: ShareFetchRequestSettings,
) -> Result<(), ShareFetchRequestFailure> {
    validate_identity(group_id, member_id)?;
    if session_epoch < 0 {
        return Err(ShareFetchRequestFailure::SessionEpoch(session_epoch));
    }
    if settings.min_bytes > settings.max_bytes {
        return Err(ShareFetchRequestFailure::MinBytesExceedMaxBytes {
            min_bytes: settings.min_bytes,
            max_bytes: settings.max_bytes,
        });
    }
    Ok(())
}

fn validate_identity(group_id: &str, member_id: &str) -> Result<(), ShareFetchRequestFailure> {
    if group_id.is_empty() || group_id.len() > MAX_KAFKA_STRING_BYTES {
        return Err(ShareFetchRequestFailure::GroupId);
    }
    if member_id.is_empty() || member_id.len() > MAX_KAFKA_STRING_BYTES {
        return Err(ShareFetchRequestFailure::MemberId);
    }
    Ok(())
}

fn materialize_topics(
    source: Vec<ShareFetchRequestTopic>,
) -> Result<Vec<FetchTopic>, ShareFetchRequestFailure> {
    let mut topics = Vec::new();
    topics
        .try_reserve_exact(source.len())
        .map_err(|_| ShareFetchRequestFailure::Allocation)?;
    for topic in source {
        let mut generated = FetchTopic::default();
        generated.topic_id = Uuid::from_bytes(topic.topic_id);
        generated.partitions = materialize_partitions(topic.partitions)?;
        topics.push(generated);
    }
    Ok(topics)
}

fn materialize_forgotten(
    source: Vec<ShareFetchRequestTopic>,
) -> Result<Vec<ForgottenTopic>, ShareFetchRequestFailure> {
    let mut topics = Vec::new();
    topics
        .try_reserve_exact(source.len())
        .map_err(|_| ShareFetchRequestFailure::Allocation)?;
    for topic in source {
        let mut generated = ForgottenTopic::default();
        generated.topic_id = Uuid::from_bytes(topic.topic_id);
        generated.partitions = materialize_partition_indexes(topic.partitions)?;
        topics.push(generated);
    }
    Ok(topics)
}

fn materialize_partitions(
    source: Vec<u32>,
) -> Result<Vec<FetchPartition>, ShareFetchRequestFailure> {
    let mut partitions = Vec::new();
    partitions
        .try_reserve_exact(source.len())
        .map_err(|_| ShareFetchRequestFailure::Allocation)?;
    for partition in source {
        let mut generated = FetchPartition::default();
        generated.partition_index = i32::try_from(partition)
            .map_err(|_| ShareFetchRequestFailure::PartitionOutOfRange(partition))?;
        partitions.push(generated);
    }
    Ok(partitions)
}

fn materialize_partition_indexes(source: Vec<u32>) -> Result<Vec<i32>, ShareFetchRequestFailure> {
    let mut partitions = Vec::new();
    partitions
        .try_reserve_exact(source.len())
        .map_err(|_| ShareFetchRequestFailure::Allocation)?;
    for partition in source {
        partitions.push(
            i32::try_from(partition)
                .map_err(|_| ShareFetchRequestFailure::PartitionOutOfRange(partition))?,
        );
    }
    Ok(partitions)
}

fn to_i32(
    value: u32,
    failure: fn(u32) -> ShareFetchRequestFailure,
) -> Result<i32, ShareFetchRequestFailure> {
    i32::try_from(value).map_err(|_| failure(value))
}

fn to_positive(
    value: u32,
    failure: fn(u32) -> ShareFetchRequestFailure,
) -> Result<i32, ShareFetchRequestFailure> {
    let value = to_i32(value, failure)?;
    if value == 0 {
        Err(failure(0))
    } else {
        Ok(value)
    }
}
