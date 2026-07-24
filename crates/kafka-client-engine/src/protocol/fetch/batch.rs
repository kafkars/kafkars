//! Record-batch decoding and absolute engine descriptor materialization.

use bytes::Bytes;
use kafka_wire_records::{Compression, RecordBatch, TimestampType};

use super::{
    failure::FetchDecodeFailure,
    limits::FetchBudget,
    model::{FetchBatch, FetchHeader, FetchRecord, FetchTimestampType},
};

pub(super) fn decode_batches(
    mut bytes: Bytes,
    topic: usize,
    partition: usize,
    budget: &mut FetchBudget,
) -> Result<Vec<FetchBatch>, FetchDecodeFailure> {
    let mut batches = Vec::new();
    while !bytes.is_empty() {
        let batch_index = batches.len();
        let decoded =
            RecordBatch::decode(&mut bytes, budget.record_limits()).map_err(|source| {
                FetchDecodeFailure::RecordBatch {
                    topic,
                    partition,
                    batch: batch_index,
                    source,
                }
            })?;
        budget.add_batch(decoded.compression != Compression::None)?;
        batches.push(normalize_batch(decoded, budget)?);
    }
    Ok(batches)
}

pub(super) fn normalize_batch(
    batch: RecordBatch,
    budget: &mut FetchBudget,
) -> Result<FetchBatch, FetchDecodeFailure> {
    if batch.base_offset < 0 {
        return Err(FetchDecodeFailure::NegativeBaseOffset {
            actual: batch.base_offset,
        });
    }
    if batch.last_offset_delta < 0 {
        return Err(FetchDecodeFailure::NegativeLastOffsetDelta {
            actual: batch.last_offset_delta,
        });
    }
    let last_offset = batch
        .base_offset
        .checked_add(i64::from(batch.last_offset_delta))
        .ok_or(FetchDecodeFailure::OffsetOverflow)?;
    let mut records = Vec::with_capacity(batch.records.len());
    let mut previous_offset = None;
    for record in batch.records {
        let offset = batch
            .base_offset
            .checked_add(i64::from(record.offset_delta))
            .ok_or(FetchDecodeFailure::OffsetOverflow)?;
        if !(batch.base_offset..=last_offset).contains(&offset) {
            return Err(FetchDecodeFailure::RecordOffsetOutsideBatch {
                offset,
                first: batch.base_offset,
                last: last_offset,
            });
        }
        if let Some(previous) = previous_offset
            && offset <= previous
        {
            return Err(FetchDecodeFailure::RecordOffsetsNotIncreasing {
                previous,
                actual: offset,
            });
        }
        previous_offset = Some(offset);
        let timestamp = batch
            .base_timestamp
            .checked_add(record.timestamp_delta)
            .ok_or(FetchDecodeFailure::TimestampOverflow)?;
        let logical_bytes = record_bytes(&record)?;
        budget.add_record(record.headers.len(), logical_bytes)?;
        records.push(FetchRecord {
            attributes: record.attributes,
            offset,
            timestamp,
            key: record.key,
            value: record.value,
            headers: record
                .headers
                .into_iter()
                .map(|header| FetchHeader {
                    key: header.key.into_bytes(),
                    value: header.value,
                })
                .collect(),
        });
    }
    Ok(FetchBatch {
        base_offset: batch.base_offset,
        last_offset,
        partition_leader_epoch: nonnegative_i32(batch.partition_leader_epoch),
        timestamp_type: match batch.timestamp_type {
            TimestampType::CreateTime => FetchTimestampType::Create,
            TimestampType::LogAppendTime => FetchTimestampType::LogAppend,
        },
        max_timestamp: batch.max_timestamp,
        producer_id: nonnegative_i64(batch.producer_id),
        producer_epoch: nonnegative_i16(batch.producer_epoch),
        base_sequence: nonnegative_i32(batch.base_sequence),
        is_transactional: batch.is_transactional,
        is_control: batch.is_control,
        records,
    })
}

fn record_bytes(record: &kafka_wire_records::Record) -> Result<usize, FetchDecodeFailure> {
    record
        .key
        .as_ref()
        .map_or(0, Bytes::len)
        .checked_add(record.value.as_ref().map_or(0, Bytes::len))
        .and_then(|bytes| {
            record.headers.iter().try_fold(bytes, |total, header| {
                total.checked_add(header.key.len()).and_then(|value| {
                    value.checked_add(header.value.as_ref().map_or(0, Bytes::len))
                })
            })
        })
        .ok_or(FetchDecodeFailure::AccountingOverflow)
}

const fn nonnegative_i64(value: i64) -> Option<i64> {
    if value < 0 { None } else { Some(value) }
}

const fn nonnegative_i32(value: i32) -> Option<i32> {
    if value < 0 { None } else { Some(value) }
}

const fn nonnegative_i16(value: i16) -> Option<i16> {
    if value < 0 { None } else { Some(value) }
}
