//! Fallible engine mirrors beside exact facade producer-record owners.

use std::sync::Arc;

use kafka_client_engine::{
    ProducerHeader as EngineProducerHeader, ProducerRecord as EngineProducerRecord,
};

use crate::record::{Header, Record};

/// One exact facade record retained beside its engine mirror until admission.
pub(crate) struct PreparedEngineRecord {
    original: Record,
    engine: EngineProducerRecord,
}

impl PreparedEngineRecord {
    pub(crate) fn into_parts(self) -> (Record, EngineProducerRecord) {
        (self.original, self.engine)
    }
}

/// One exact facade vector retained beside its engine mirrors until admission.
pub(crate) struct PreparedEngineRecords {
    originals: Vec<Record>,
    engine: Vec<EngineProducerRecord>,
}

impl PreparedEngineRecords {
    pub(crate) fn into_parts(self) -> (Vec<Record>, Vec<EngineProducerRecord>) {
        (self.originals, self.engine)
    }
}

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

#[allow(
    clippy::result_large_err,
    reason = "conversion allocation rejection returns the exact facade record"
)]
pub(crate) fn prepare_engine_record(original: Record) -> Result<PreparedEngineRecord, Record> {
    let header_count = original.headers().len();
    prepare_engine_record_with_header_capacity(original, header_count)
}

pub(crate) fn prepare_engine_records(
    originals: Vec<Record>,
) -> Result<PreparedEngineRecords, Vec<Record>> {
    let mut engine = Vec::new();
    if engine.try_reserve_exact(originals.len()).is_err() {
        return Err(originals);
    }
    for original in &originals {
        let Ok(record) = mirror_engine_record(original, original.headers().len()) else {
            return Err(originals);
        };
        engine.push(record);
    }
    Ok(PreparedEngineRecords { originals, engine })
}

#[allow(
    clippy::result_large_err,
    reason = "forced allocation rejection returns the exact facade record"
)]
pub(super) fn prepare_engine_record_with_header_capacity(
    original: Record,
    header_capacity: usize,
) -> Result<PreparedEngineRecord, Record> {
    match mirror_engine_record(&original, header_capacity) {
        Ok(engine) => Ok(PreparedEngineRecord { original, engine }),
        Err(()) => Err(original),
    }
}

fn mirror_engine_record(
    original: &Record,
    header_capacity: usize,
) -> Result<EngineProducerRecord, ()> {
    if header_capacity < original.headers().len() {
        return Err(());
    }
    let mut headers = Vec::new();
    headers
        .try_reserve_exact(header_capacity)
        .map_err(|_error| ())?;
    for header in original.headers() {
        headers.push(mirror_engine_header(header));
    }

    let record = EngineProducerRecord::to(Arc::clone(original.topic_owner()));
    let record = match original.expected_topic_uuid_value() {
        Some(topic_uuid) => record.expected_topic_uuid(topic_uuid.into_bytes()),
        None => record,
    };
    let record = match original.explicit_partition() {
        Some(partition) => record.partition(partition),
        None => record,
    };
    let record = match original.timestamp() {
        Some(timestamp) => record.timestamp_milliseconds(timestamp),
        None => record,
    };
    let record = match original.key_bytes() {
        Some(key) => record.key(key.clone()),
        None => record,
    };
    let record = match original.value_bytes() {
        Some(value) => record.value(value.clone()),
        None => record,
    };
    let record = record.with_headers(headers);
    match original.shared_source_owner() {
        Some(owner) => Ok(record.retain_source_owner(owner)),
        None => Ok(record),
    }
}

fn mirror_engine_header(header: &Header) -> EngineProducerHeader {
    let (name, value) = header.clone().into_parts();
    let (name, source_owner) = name.into_shared_parts();
    let header = EngineProducerHeader::try_from_shared_name(name, value)
        .unwrap_or_else(|_error| unreachable!("facade header name was validated"));
    match source_owner.into_arc() {
        Some(owner) => header.retain_source_owner(owner),
        None => header,
    }
}
