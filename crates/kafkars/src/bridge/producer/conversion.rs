//! Lossless conversion between facade and engine producer records.

use kafka_client_engine::{
    ProducerHeader as EngineProducerHeader, ProducerRecord as EngineProducerRecord,
};

use crate::{
    header_name::SourceOwner,
    record::{Header, Record, RecordTransferParts},
};

pub(crate) fn validate_batch_records(
    records: &[Record],
) -> Result<(), kafka_client_engine::ProducerTrySendErrorKind> {
    for record in records {
        if record.topic().is_empty() {
            return Err(kafka_client_engine::ProducerTrySendErrorKind::EmptyTopic);
        }
        if record
            .explicit_partition()
            .is_some_and(|partition| partition < 0)
        {
            return Err(kafka_client_engine::ProducerTrySendErrorKind::NegativeExplicitPartition);
        }
    }
    Ok(())
}

pub(crate) fn into_engine_record(record: Record) -> EngineProducerRecord {
    let RecordTransferParts {
        topic,
        partition,
        timestamp_milliseconds,
        key,
        value,
        headers,
        source_owner,
    } = record.into_transfer_parts();
    let record = EngineProducerRecord::to(topic);
    let record = match partition {
        Some(partition) => record.partition(partition),
        None => record,
    };
    let record = match timestamp_milliseconds {
        Some(timestamp) => record.timestamp_milliseconds(timestamp),
        None => record,
    };
    let record = match key {
        Some(key) => record.key(key),
        None => record,
    };
    let record = match value {
        Some(value) => record.value(value),
        None => record,
    };
    let record = headers.into_iter().fold(record, |record, header| {
        record.header(into_engine_header(header))
    });
    match source_owner.into_arc() {
        Some(owner) => record.retain_source_owner(owner),
        None => record,
    }
}

pub(crate) fn restore_rejected_record(record: EngineProducerRecord) -> Record {
    let (topic, partition, timestamp_milliseconds, key, value, headers, source_owner) =
        record.into_shared_parts();
    let headers = headers
        .into_iter()
        .map(|header| {
            let (name, value, source_owner) = header.into_shared_parts();
            Header::from_shared_parts(name, value, SourceOwner::from_optional(source_owner))
        })
        .collect();
    Record::from_transfer_parts(RecordTransferParts {
        topic,
        partition,
        timestamp_milliseconds,
        key,
        value,
        headers,
        source_owner: SourceOwner::from_optional(source_owner),
    })
}

fn into_engine_header(header: Header) -> EngineProducerHeader {
    let (name, value) = header.into_parts();
    let (name, source_owner) = name.into_shared_parts();
    let header = EngineProducerHeader::try_from_shared_name(name, value)
        .unwrap_or_else(|_error| unreachable!("facade header name was validated"));
    match source_owner.into_arc() {
        Some(owner) => header.retain_source_owner(owner),
        None => header,
    }
}
