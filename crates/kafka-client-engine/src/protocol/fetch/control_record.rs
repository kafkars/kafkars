//! Strict composition of generated Kafka transaction-control schemas.

use kafka_wire::{ControlRecordTypeSchema, EndTxnMarker};
use kafka_wire_core::{ApiVersion, DecodeError, DecodeLimits, Decoder, KafkaDecode};

use super::model::FetchBatch;

const ABORT: i16 = 0;
const COMMIT: i16 = 1;

/// Transaction marker represented by one decoded control batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FetchControlRecordKind {
    Abort,
    Commit,
    Other(i16),
}

/// Why decoded control-batch bytes cannot represent one Kafka marker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FetchControlRecordFailure {
    RecordCount { actual: usize },
    MissingKey,
    MissingValue,
    Key(DecodeError),
    Value(DecodeError),
}

/// Decodes one transaction marker through kafka-wire's generated schemas.
pub(super) fn decode_control_record(
    batch: &FetchBatch,
) -> Result<FetchControlRecordKind, FetchControlRecordFailure> {
    let [record] = batch.records.as_slice() else {
        return Err(FetchControlRecordFailure::RecordCount {
            actual: batch.records.len(),
        });
    };
    let key = record
        .key
        .clone()
        .ok_or(FetchControlRecordFailure::MissingKey)?;
    let value = record
        .value
        .clone()
        .ok_or(FetchControlRecordFailure::MissingValue)?;
    let marker =
        decode_versioned::<ControlRecordTypeSchema>(key).map_err(FetchControlRecordFailure::Key)?;
    let _value =
        decode_versioned::<EndTxnMarker>(value).map_err(FetchControlRecordFailure::Value)?;
    Ok(match marker.type_ {
        ABORT => FetchControlRecordKind::Abort,
        COMMIT => FetchControlRecordKind::Commit,
        other => FetchControlRecordKind::Other(other),
    })
}

fn decode_versioned<T>(bytes: bytes::Bytes) -> Result<T, DecodeError>
where
    T: KafkaDecode,
{
    let mut decoder = Decoder::new(bytes, DecodeLimits::default())?;
    let version = ApiVersion::new(decoder.read_i16()?);
    let value = T::decode(&mut decoder, version)?;
    decoder.finish()?;
    Ok(value)
}
