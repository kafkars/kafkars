//! Lossless record ownership transfer across the private facade-engine seam.

use kafka_client_engine::{
    ProducerHeader as EngineProducerHeader, ProducerRecord as EngineProducerRecord,
};

use crate::record::{Header, Record, RecordParts};

pub(crate) fn into_engine_record(record: Record) -> EngineProducerRecord {
    let RecordParts {
        topic,
        partition,
        timestamp_milliseconds,
        key,
        value,
        headers,
    } = record.into_parts();
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
    headers.into_iter().fold(record, |record, header| {
        record.header(into_engine_header(header))
    })
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "a rejected engine record transfers ownership back to the facade"
)]
pub(crate) fn restore_rejected_record(record: EngineProducerRecord) -> Record {
    let headers = record
        .headers()
        .iter()
        .map(|header| Header::from_parts(header.name().to_owned(), header.value().cloned()))
        .collect();
    Record::from_parts(RecordParts {
        topic: record.topic().to_owned(),
        partition: record.explicit_partition(),
        timestamp_milliseconds: record.timestamp(),
        key: record.key_bytes().cloned(),
        value: record.value_bytes().cloned(),
        headers,
    })
}

fn into_engine_header(header: Header) -> EngineProducerHeader {
    let (name, value) = header.into_parts();
    match value {
        Some(value) => EngineProducerHeader::new(name, value),
        None => EngineProducerHeader::null(name),
    }
}
