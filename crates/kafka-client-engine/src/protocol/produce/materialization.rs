//! Mechanical `RecordBatch` encoding for ordinary and transactional Produce.

use bytes::Bytes;
use kafka_client_core::CompressionPolicy;
use kafka_wire_records::{
    Compression, Record, RecordBatch, RecordEncodeLimits, RecordHeader, TimestampType,
};

use crate::{
    producer::materialization::{
        MaterializationBatch, MaterializationRecord, TransactionalMaterializationBatch,
    },
    protocol::error::ProduceMaterializationError,
};

use super::MaterializedProduce;

const NO_LEADER_EPOCH: i32 = -1;

/// Uses the sibling wire crates to materialize one uncompressed Produce batch.
pub(crate) fn materialize_explicit_produce_batch(
    input: MaterializationBatch,
) -> Result<MaterializedProduce, ProduceMaterializationError> {
    materialize_explicit_produce_batch_with_compression(input, CompressionPolicy::None)
}

/// Uses the sibling wire crates for the complete `RecordBatch` codec operation.
pub(crate) fn materialize_explicit_produce_batch_with_compression(
    input: MaterializationBatch,
    compression: CompressionPolicy,
) -> Result<MaterializedProduce, ProduceMaterializationError> {
    let (topic, partition, leader_broker_id, records, max_batch_bytes, identity, sequence) =
        input.into_idempotent_parts();
    let records = record_batch(
        records,
        identity.producer_id(),
        identity.producer_epoch(),
        sequence.base_sequence(),
        sequence.record_count(),
        max_batch_bytes,
        compression,
        false,
    )?;

    Ok(MaterializedProduce::new(
        topic,
        partition,
        leader_broker_id,
        records,
    ))
}

/// Encodes one transaction-fenced batch without adding coordinator or retry policy.
pub(crate) fn materialize_transactional_produce_batch(
    input: TransactionalMaterializationBatch,
    compression: CompressionPolicy,
) -> Result<MaterializedProduce, ProduceMaterializationError> {
    let (topic, partition, records, max_batch_bytes, identity, sequence) = input.into_parts();
    let records = record_batch(
        records,
        identity.producer_id(),
        identity.producer_epoch(),
        sequence.base_sequence(),
        sequence.record_count(),
        max_batch_bytes,
        compression,
        true,
    )?;

    Ok(MaterializedProduce::new(topic, partition, None, records))
}

#[expect(
    clippy::too_many_arguments,
    reason = "record-batch materialization keeps exact identity, sequence, byte, codec, and mode inputs explicit"
)]
fn record_batch(
    records: Vec<MaterializationRecord>,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    sequence_record_count: u32,
    max_batch_bytes: usize,
    compression: CompressionPolicy,
    is_transactional: bool,
) -> Result<Bytes, ProduceMaterializationError> {
    let Some(base_timestamp) = records
        .first()
        .map(MaterializationRecord::timestamp_ms_for_protocol)
    else {
        return Err(ProduceMaterializationError::empty_batch());
    };
    let last_offset = records.len().saturating_sub(1);
    let last_offset_delta = i32::try_from(last_offset)
        .map_err(|_| ProduceMaterializationError::record_count_overflow(records.len()))?;
    if usize::try_from(sequence_record_count) != Ok(records.len()) {
        return Err(ProduceMaterializationError::record_count_overflow(
            records.len(),
        ));
    }
    let max_timestamp = records
        .iter()
        .map(MaterializationRecord::timestamp_ms_for_protocol)
        .max()
        .unwrap_or(base_timestamp);
    let records = records
        .into_iter()
        .enumerate()
        .map(|(offset, record)| wire_record(base_timestamp, offset, record))
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch {
        base_offset: 0,
        last_offset_delta,
        partition_leader_epoch: NO_LEADER_EPOCH,
        compression: wire_compression(compression),
        timestamp_type: TimestampType::CreateTime,
        is_transactional,
        is_control: false,
        has_delete_horizon: false,
        base_timestamp,
        max_timestamp,
        producer_id,
        producer_epoch,
        base_sequence,
        records,
    }
    .encode_to_bytes(RecordEncodeLimits::new(max_batch_bytes, max_batch_bytes))
    .map_err(ProduceMaterializationError::record)
}

const fn wire_compression(compression: CompressionPolicy) -> Compression {
    match compression {
        CompressionPolicy::None => Compression::None,
        CompressionPolicy::Gzip => Compression::Gzip,
        CompressionPolicy::Snappy => Compression::Snappy,
        CompressionPolicy::Lz4 => Compression::Lz4,
        CompressionPolicy::Zstd => Compression::Zstd,
    }
}

fn wire_record(
    base_timestamp: i64,
    offset: usize,
    record: MaterializationRecord,
) -> Result<Record, ProduceMaterializationError> {
    let (timestamp_ms, key, value, headers) = record.into_parts();
    let timestamp_delta = timestamp_ms.checked_sub(base_timestamp).ok_or_else(|| {
        ProduceMaterializationError::timestamp_delta_overflow(base_timestamp, timestamp_ms)
    })?;
    let offset_delta = i32::try_from(offset).map_err(|_| {
        ProduceMaterializationError::record_count_overflow(offset.saturating_add(1))
    })?;

    let headers = headers
        .into_iter()
        .map(|header| {
            let (name, value) = header.into_parts();
            Ok(RecordHeader {
                key: name
                    .try_into()
                    .map_err(ProduceMaterializationError::invalid_header_name)?,
                value,
            })
        })
        .collect::<Result<Vec<_>, ProduceMaterializationError>>()?;

    Ok(Record {
        attributes: 0,
        timestamp_delta,
        offset_delta,
        key,
        value,
        headers,
    })
}
