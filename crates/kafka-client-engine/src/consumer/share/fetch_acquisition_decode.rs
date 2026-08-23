//! Bounded decoding and exact range correlation before share-ledger admission.

use kafka_client_core::{
    AssignedTopicPartition, ByteCount, Deadline, Moment, ShareAcquiredOffsets, ShareAcquiredRange,
    ShareAcquiredRangeError, ShareDeliveryCount, ShareTopicUuid,
};

use crate::protocol::{
    consumer::share_fetch::{ShareFetchEndpoint, ShareFetchSuccess},
    fetch::{
        FetchBatch, FetchDecodeFailure, FetchDecodeLimits, FetchRecord, decode_record_payload,
    },
};

use super::fetch_plan::ShareFetchSessionRequestPlan;

/// One decoded partition retaining all record bytes until ledger admission succeeds.
#[must_use = "decoded share records must enter delivery ownership or be released"]
pub(super) struct DecodedShareFetchPartition {
    pub(super) topic_uuid: ShareTopicUuid,
    pub(super) partition: AssignedTopicPartition,
    pub(super) batches: Vec<FetchBatch>,
}

/// Complete successful response facts prepared for one atomic core settlement.
#[must_use = "decoded ShareFetch success must settle its session or be released"]
pub(super) struct DecodedShareFetchSuccess {
    pub(super) throttle_time_ms: u32,
    pub(super) acquisition_lock_timeout_ms: Option<u32>,
    pub(super) endpoints: Vec<ShareFetchEndpoint>,
    pub(super) ranges: Vec<ShareAcquiredRange>,
    pub(super) partitions: Vec<DecodedShareFetchPartition>,
}

pub(super) fn decode_share_fetch_success(
    success: ShareFetchSuccess,
    plan: &ShareFetchSessionRequestPlan,
    lock_deadline: Deadline,
    now: Moment,
    limits: FetchDecodeLimits,
) -> Result<DecodedShareFetchSuccess, ShareFetchAcquisitionDecodeError> {
    let mut ranges = Vec::new();
    let mut partitions = Vec::new();
    let range_capacity = usize::try_from(success.retained_records)
        .map_err(|_error| ShareFetchAcquisitionDecodeError::Accounting)?;
    ranges
        .try_reserve_exact(range_capacity)
        .map_err(|_error| ShareFetchAcquisitionDecodeError::Allocation)?;
    for topic in success.topics {
        let topic_uuid = ShareTopicUuid::try_from_bytes(topic.topic_id)
            .ok_or(ShareFetchAcquisitionDecodeError::TopicIdentity)?;
        partitions
            .try_reserve(topic.partitions.len())
            .map_err(|_error| ShareFetchAcquisitionDecodeError::Allocation)?;
        for partition in topic.partitions {
            if partition.rejection.is_some() {
                return Err(ShareFetchAcquisitionDecodeError::PartitionRejected);
            }
            let local = plan
                .resolve_partition(topic.topic_id, partition.partition)
                .ok_or(ShareFetchAcquisitionDecodeError::UnknownPartition)?;
            let decoded = decode_record_payload(partition.records, limits)
                .map_err(ShareFetchAcquisitionDecodeError::Records)?;
            let mut charges = correlate_range_bytes(&decoded.batches, &partition.acquired)?;
            let logical_bytes = charges
                .iter()
                .try_fold(0usize, |total, charge| total.checked_add(*charge));
            if logical_bytes != Some(decoded.logical_bytes) {
                return Err(ShareFetchAcquisitionDecodeError::Accounting);
            }
            if let Some(first) = charges.first_mut() {
                let overhead = decoded
                    .retained_bytes
                    .checked_sub(decoded.logical_bytes)
                    .ok_or(ShareFetchAcquisitionDecodeError::Accounting)?;
                *first = first
                    .checked_add(overhead)
                    .ok_or(ShareFetchAcquisitionDecodeError::Accounting)?;
            } else if decoded.records != 0 {
                return Err(ShareFetchAcquisitionDecodeError::Accounting);
            }
            let has_acquisitions = !partition.acquired.is_empty();
            for (source, retained_bytes) in partition.acquired.into_iter().zip(charges) {
                let offsets =
                    ShareAcquiredOffsets::try_new(source.first_offset, source.last_offset)
                        .map_err(ShareFetchAcquisitionDecodeError::Range)?;
                let delivery_count = ShareDeliveryCount::try_from_raw(source.delivery_count)
                    .ok_or(ShareFetchAcquisitionDecodeError::DeliveryCount)?;
                let retained_bytes = u64::try_from(retained_bytes)
                    .map_err(|_error| ShareFetchAcquisitionDecodeError::Accounting)?;
                ranges.push(
                    ShareAcquiredRange::try_new(
                        topic_uuid,
                        local,
                        offsets,
                        delivery_count,
                        ByteCount::new(retained_bytes),
                        lock_deadline,
                        now,
                    )
                    .map_err(ShareFetchAcquisitionDecodeError::Range)?,
                );
            }
            if !decoded.batches.is_empty() && has_acquisitions {
                partitions.push(DecodedShareFetchPartition {
                    topic_uuid,
                    partition: local,
                    batches: decoded.batches,
                });
            }
        }
    }
    Ok(DecodedShareFetchSuccess {
        throttle_time_ms: success.throttle_time_ms,
        acquisition_lock_timeout_ms: success.acquisition_lock_timeout_ms,
        endpoints: success.endpoints,
        ranges,
        partitions,
    })
}

fn correlate_range_bytes(
    batches: &[FetchBatch],
    acquired: &[crate::protocol::consumer::share_fetch::ShareFetchAcquiredRange],
) -> Result<Vec<usize>, ShareFetchAcquisitionDecodeError> {
    let mut charges = Vec::new();
    charges
        .try_reserve_exact(acquired.len())
        .map_err(|_error| ShareFetchAcquisitionDecodeError::Allocation)?;
    charges.resize(acquired.len(), 0usize);
    let mut observed = Vec::new();
    observed
        .try_reserve_exact(acquired.len())
        .map_err(|_error| ShareFetchAcquisitionDecodeError::Allocation)?;
    observed.resize(acquired.len(), false);
    for batch in batches {
        if batch.is_control {
            return Err(ShareFetchAcquisitionDecodeError::ControlBatch);
        }
        for record in &batch.records {
            let index = acquired
                .iter()
                .position(|range| (range.first_offset..=range.last_offset).contains(&record.offset))
                .ok_or(ShareFetchAcquisitionDecodeError::UnacquiredRecord(
                    record.offset,
                ))?;
            charges[index] = charges[index]
                .checked_add(record_payload_bytes(record)?)
                .ok_or(ShareFetchAcquisitionDecodeError::Accounting)?;
            observed[index] = true;
        }
    }
    if observed.iter().any(|observed| !observed) {
        return Err(ShareFetchAcquisitionDecodeError::EmptyAcquiredRange);
    }
    Ok(charges)
}

fn record_payload_bytes(record: &FetchRecord) -> Result<usize, ShareFetchAcquisitionDecodeError> {
    record
        .key
        .as_ref()
        .map_or(0, bytes::Bytes::len)
        .checked_add(record.value.as_ref().map_or(0, bytes::Bytes::len))
        .and_then(|bytes| {
            record.headers.iter().try_fold(bytes, |total, header| {
                total.checked_add(header.key.len()).and_then(|value| {
                    value.checked_add(header.value.as_ref().map_or(0, bytes::Bytes::len))
                })
            })
        })
        .ok_or(ShareFetchAcquisitionDecodeError::Accounting)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ShareFetchAcquisitionDecodeError {
    Allocation,
    TopicIdentity,
    UnknownPartition,
    PartitionRejected,
    Records(FetchDecodeFailure),
    ControlBatch,
    UnacquiredRecord(i64),
    EmptyAcquiredRange,
    DeliveryCount,
    Range(ShareAcquiredRangeError),
    Accounting,
}
