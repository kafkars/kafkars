//! Record-layer Fetch failure classification owned by response normalization.

use kafka_wire_core::{DecodeError, EncodeError};
use kafka_wire_records::RecordError;

use super::outcome_failure::FetchOutcomeFailureClass;

pub(super) fn classify_record_error(failure: &RecordError) -> FetchOutcomeFailureClass {
    #[allow(
        clippy::match_same_arms,
        reason = "named variants document the audited catalog while the wildcard safely fences future non-exhaustive variants"
    )]
    match failure {
        RecordError::Wire(failure) => classify_wire_decode_error(failure),
        RecordError::Encode(failure) => classify_wire_encode_error(failure),
        RecordError::UnsupportedMagic { .. } => FetchOutcomeFailureClass::Compatibility,
        RecordError::BatchLimitExceeded { .. }
        | RecordError::UncompressedRecordsLimitExceeded { .. }
        | RecordError::DecompressionLimitExceeded { .. }
        | RecordError::RetainedPayloadLimitExceeded { .. } => {
            FetchOutcomeFailureClass::ResponseTooLarge
        }
        RecordError::NegativeBatchLength { .. }
        | RecordError::CorruptBatch { .. }
        | RecordError::TruncatedBatch { .. }
        | RecordError::RecordCountMismatch { .. }
        | RecordError::TrailingRecordBytes { .. }
        | RecordError::NegativeRecordCount { .. }
        | RecordError::NegativeRecordLength { .. }
        | RecordError::NegativeHeaderCount { .. }
        | RecordError::RecordSizeMismatch { .. }
        | RecordError::InvalidRecordFieldLength { .. }
        | RecordError::NullHeaderKey
        | RecordError::CompressionFailed { .. }
        | RecordError::UnknownCompression { .. }
        | RecordError::UnknownBatchAttributes { .. } => FetchOutcomeFailureClass::InvalidResponse,
        _ => FetchOutcomeFailureClass::InvalidResponse,
    }
}

fn classify_wire_encode_error(failure: &EncodeError) -> FetchOutcomeFailureClass {
    match failure {
        EncodeError::UnsupportedVersion { .. }
        | EncodeError::FieldNotRepresentable { .. }
        | EncodeError::NullNotAllowed { .. }
        | EncodeError::TaggedFieldsNotRepresentable { .. } => {
            FetchOutcomeFailureClass::Compatibility
        }
        _ => FetchOutcomeFailureClass::DriverRejected,
    }
}

fn classify_wire_decode_error(failure: &DecodeError) -> FetchOutcomeFailureClass {
    match failure {
        DecodeError::UnsupportedVersion { .. } => FetchOutcomeFailureClass::Compatibility,
        DecodeError::LimitExceeded { .. } | DecodeError::LengthOverflow { .. } => {
            FetchOutcomeFailureClass::ResponseTooLarge
        }
        _ => FetchOutcomeFailureClass::InvalidResponse,
    }
}
