//! Semantic validation and retained materialization of one decoded batch.

use bytes::Bytes;
use kafka_wire_records::{RecordBatch, TimestampType};

use super::{
    batch_identity::producer_identity,
    failure::FetchDecodeFailure,
    limits::FetchBudget,
    model::{FetchBatch, FetchHeader, FetchRecord, FetchTimestampType},
};

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
    let next_offset = last_offset
        .checked_add(1)
        .ok_or(FetchDecodeFailure::NextOffsetOverflow { last_offset })?;
    let records_empty = batch.records.is_empty();
    let partition_leader_epoch = optional_epoch(batch.partition_leader_epoch)?;
    let (base_timestamp, max_timestamp) = batch_timestamps(
        batch.base_timestamp,
        batch.max_timestamp,
        batch.timestamp_type,
        batch.has_delete_horizon,
        records_empty,
    )?;
    let delete_horizon_ms = batch.has_delete_horizon.then_some(batch.base_timestamp);
    let producer = producer_identity(
        batch.producer_id,
        batch.producer_epoch,
        batch.base_sequence,
        batch.is_transactional,
        batch.is_control,
    )?;
    let mut records = Vec::with_capacity(batch.records.len());
    let mut previous_offset = None;
    for record in batch.records {
        let offset = record_offset(
            batch.base_offset,
            last_offset,
            record.offset_delta,
            previous_offset,
        )?;
        previous_offset = Some(offset);
        let timestamp = record_timestamp(
            batch.timestamp_type,
            base_timestamp,
            max_timestamp,
            record.timestamp_delta,
        )?;
        budget.add_record(record.headers.len(), record_bytes(&record)?)?;
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
        next_offset,
        partition_leader_epoch,
        timestamp_type: match batch.timestamp_type {
            TimestampType::CreateTime => FetchTimestampType::Create,
            TimestampType::LogAppendTime => FetchTimestampType::LogAppend,
        },
        max_timestamp,
        producer,
        is_transactional: batch.is_transactional,
        is_control: batch.is_control,
        delete_horizon_ms,
        records,
    })
}

fn record_offset(
    base: i64,
    last: i64,
    delta: i32,
    previous: Option<i64>,
) -> Result<i64, FetchDecodeFailure> {
    let offset = base
        .checked_add(i64::from(delta))
        .ok_or(FetchDecodeFailure::OffsetOverflow)?;
    if !(base..=last).contains(&offset) {
        return Err(FetchDecodeFailure::RecordOffsetOutsideBatch {
            offset,
            first: base,
            last,
        });
    }
    if let Some(previous) = previous
        && offset <= previous
    {
        return Err(FetchDecodeFailure::RecordOffsetsNotIncreasing {
            previous,
            actual: offset,
        });
    }
    Ok(offset)
}

fn batch_timestamps(
    base: i64,
    max: i64,
    timestamp_type: TimestampType,
    has_delete_horizon: bool,
    records_empty: bool,
) -> Result<(Option<i64>, Option<i64>), FetchDecodeFailure> {
    match (base, max) {
        (-1, -1) if !has_delete_horizon && records_empty => Ok((None, None)),
        (-1, max) if !has_delete_horizon && records_empty && max >= 0 => Ok((None, Some(max))),
        (base, max)
            if base >= 0
                && max >= 0
                && (has_delete_horizon
                    || timestamp_type == TimestampType::LogAppendTime
                    || max >= base) =>
        {
            Ok((Some(base), Some(max)))
        }
        _ => Err(FetchDecodeFailure::InvalidBatchTimestamps {
            base_timestamp: base,
            max_timestamp: max,
        }),
    }
}

fn record_timestamp(
    timestamp_type: TimestampType,
    base: Option<i64>,
    max: Option<i64>,
    delta: i64,
) -> Result<Option<i64>, FetchDecodeFailure> {
    let (Some(base), Some(max)) = (base, max) else {
        return if delta == 0 {
            Ok(None)
        } else {
            Err(FetchDecodeFailure::TimestampDeltaWithoutTimestamp { actual: delta })
        };
    };
    if timestamp_type == TimestampType::LogAppendTime {
        return Ok(Some(max));
    }
    let timestamp = base
        .checked_add(delta)
        .ok_or(FetchDecodeFailure::TimestampOverflow)?;
    if timestamp < 0 {
        return Err(FetchDecodeFailure::NegativeRecordTimestamp { actual: timestamp });
    }
    if timestamp > max {
        return Err(FetchDecodeFailure::RecordTimestampAfterBatchMax {
            actual: timestamp,
            max_timestamp: max,
        });
    }
    Ok(Some(timestamp))
}

fn optional_epoch(value: i32) -> Result<Option<i32>, FetchDecodeFailure> {
    match value {
        -1 => Ok(None),
        value if value >= 0 => Ok(Some(value)),
        actual => Err(FetchDecodeFailure::InvalidPartitionLeaderEpoch { actual }),
    }
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
