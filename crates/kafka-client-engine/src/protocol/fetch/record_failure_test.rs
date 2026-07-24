//! Nested `RecordBatch` failure categories at the direct-consumer Fetch seam.

use kafka_wire_core::{ApiVersion, DecodeError, EncodeError, VersionRange};
use kafka_wire_records::RecordError;

use super::{
    FetchDecodeFailure, FetchOutcomeFailure, FetchResponseFailure,
    outcome_failure::{FetchOutcomeFailureClass, classify_fetch_outcome_failure},
};

#[test]
fn unsupported_record_representations_are_compatibility() {
    let failures = [
        record(RecordError::UnsupportedMagic { magic: 1 }),
        record(RecordError::Wire(DecodeError::UnsupportedVersion {
            message: "RecordBatch",
            version: ApiVersion::new(1),
            supported: VersionRange::new(2, 2),
        })),
        record(RecordError::Encode(EncodeError::FieldNotRepresentable {
            message: "RecordBatch",
            field: "records",
            version: ApiVersion::new(1),
        })),
        decode(FetchDecodeFailure::UnsupportedRecordBatchDecode {
            topic: 0,
            partition: 0,
            batch: 0,
        }),
    ];
    for failure in failures {
        assert_eq!(classify(&failure), FetchOutcomeFailureClass::Compatibility);
    }
}

#[test]
fn every_record_bound_including_nested_wire_bounds_is_response_too_large() {
    let failures = [
        RecordError::BatchLimitExceeded {
            length: 2,
            limit: 1,
        },
        RecordError::UncompressedRecordsLimitExceeded {
            length: 2,
            limit: 1,
        },
        RecordError::DecompressionLimitExceeded {
            codec: "gzip",
            limit: 1,
        },
        RecordError::RetainedPayloadLimitExceeded {
            length: 2,
            limit: 1,
        },
        RecordError::Wire(DecodeError::LimitExceeded {
            kind: "records",
            length: 2,
            limit: 1,
            offset: 0,
        }),
        RecordError::Wire(DecodeError::LengthOverflow {
            kind: "records",
            offset: 0,
        }),
    ];
    for failure in failures {
        assert_eq!(
            classify(&record(failure)),
            FetchOutcomeFailureClass::ResponseTooLarge
        );
    }
}

#[test]
fn nested_outbound_record_encoding_failures_are_driver_rejected() {
    assert_eq!(
        classify(&record(RecordError::Encode(EncodeError::SizeMismatch {
            predicted: 1,
            actual: 2,
        }))),
        FetchOutcomeFailureClass::DriverRejected
    );
}

#[test]
fn representative_record_corruption_is_invalid_response() {
    let failures = [
        RecordError::NegativeBatchLength { length: -1 },
        RecordError::CorruptBatch {
            declared: 1,
            actual: 2,
        },
        RecordError::TruncatedBatch {
            declared: 2,
            available: 1,
        },
        RecordError::RecordCountMismatch {
            declared: 2,
            actual: 1,
        },
        RecordError::TrailingRecordBytes { bytes: 1 },
        RecordError::NegativeRecordCount { count: -1 },
        RecordError::NegativeRecordLength {
            length: -1,
            offset: 0,
        },
        RecordError::NegativeHeaderCount {
            count: -1,
            offset: 0,
        },
        RecordError::RecordSizeMismatch {
            declared: 2,
            consumed: 1,
        },
        RecordError::InvalidRecordFieldLength { length: -2 },
        RecordError::NullHeaderKey,
        RecordError::CompressionFailed {
            codec: "gzip",
            detail: "invalid".to_owned(),
        },
        RecordError::UnknownCompression { codec: 7 },
        RecordError::UnknownBatchAttributes { bits: 8 },
        RecordError::Wire(DecodeError::MalformedVarint { offset: 0 }),
    ];
    for failure in failures {
        assert_eq!(
            classify(&record(failure)),
            FetchOutcomeFailureClass::InvalidResponse
        );
    }
}

fn record(failure: RecordError) -> FetchOutcomeFailure {
    decode(FetchDecodeFailure::RecordBatch {
        topic: 0,
        partition: 0,
        batch: 0,
        source: failure,
    })
}

fn decode(failure: FetchDecodeFailure) -> FetchOutcomeFailure {
    FetchOutcomeFailure::Response(FetchResponseFailure::Decode(failure))
}

fn classify(failure: &FetchOutcomeFailure) -> FetchOutcomeFailureClass {
    classify_fetch_outcome_failure(failure)
}
