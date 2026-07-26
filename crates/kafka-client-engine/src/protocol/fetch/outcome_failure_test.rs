//! Classification scenarios for bounded Fetch response normalization.

use kafka_wire_records::RecordError;

use super::{
    FetchDecodeFailure, FetchOutcomeFailure, FetchResponseFailure, FetchRetentionFailure,
    outcome_failure::{FetchOutcomeFailureClass, classify_fetch_outcome_failure},
};

#[test]
fn compatibility_is_reserved_for_the_selected_fetch_version() {
    assert_eq!(
        classify(&FetchOutcomeFailure::Response(
            FetchResponseFailure::UnsupportedApiVersion { actual: 3 }
        )),
        FetchOutcomeFailureClass::Compatibility
    );
}

#[test]
fn response_shape_and_semantic_failures_are_invalid() {
    let failures = [
        FetchOutcomeFailure::NegativeThrottleTime { actual: -1 },
        FetchOutcomeFailure::UnexpectedSessionId { actual: 1 },
        FetchOutcomeFailure::ThrottleTickOverflow {
            milliseconds: u32::MAX,
        },
        FetchOutcomeFailure::CorrelatedShapeLost,
        FetchOutcomeFailure::Response(FetchResponseFailure::TopicCount { actual: 2 }),
        FetchOutcomeFailure::Response(FetchResponseFailure::TopicNameMismatch),
        FetchOutcomeFailure::Response(FetchResponseFailure::PartitionCount { actual: 2 }),
        FetchOutcomeFailure::Response(FetchResponseFailure::PartitionIndexMismatch { actual: 2 }),
        decode(FetchDecodeFailure::MissingLastStableOffset),
    ];
    for failure in failures {
        assert_eq!(
            classify(&failure),
            FetchOutcomeFailureClass::InvalidResponse
        );
    }
}

#[test]
fn invalid_engine_request_facts_are_driver_rejections() {
    let failures = [
        FetchOutcomeFailure::InvalidRequestedOffset { actual: -1 },
        FetchOutcomeFailure::Response(FetchResponseFailure::RequestedPartitionOutOfRange {
            actual: u32::MAX,
        }),
    ];
    for failure in failures {
        assert_eq!(classify(&failure), FetchOutcomeFailureClass::DriverRejected);
    }
}

#[test]
fn stable_retention_and_decoder_bounds_are_response_too_large() {
    let failures = [
        FetchOutcomeFailure::Retention(FetchRetentionFailure::AccountingOverflow),
        FetchOutcomeFailure::Retention(FetchRetentionFailure::ReservationExceeded {
            actual: 2,
            reserved: 1,
        }),
        decode(FetchDecodeFailure::ResponseRetainedBytes {
            actual: 2,
            limit: 1,
        }),
        decode(FetchDecodeFailure::ResponseAllocations {
            actual: 2,
            limit: 1,
        }),
        decode(FetchDecodeFailure::RecordCount {
            actual: 2,
            limit: 1,
        }),
        decode(FetchDecodeFailure::LogicalRecordBytes {
            actual: 2,
            limit: 1,
        }),
        decode(FetchDecodeFailure::ReadCommittedScratch { required: 2 }),
        decode(FetchDecodeFailure::AccountingOverflow),
        decode(FetchDecodeFailure::RecordBatch {
            topic: 0,
            partition: 0,
            batch: 0,
            source: RecordError::DecompressionLimitExceeded {
                codec: "gzip",
                limit: 1,
            },
        }),
    ];
    for failure in failures {
        assert_eq!(
            classify(&failure),
            FetchOutcomeFailureClass::ResponseTooLarge
        );
    }
}

#[test]
fn malformed_record_batches_are_invalid_responses() {
    assert_eq!(
        classify(&decode(FetchDecodeFailure::RecordBatch {
            topic: 0,
            partition: 0,
            batch: 0,
            source: RecordError::CorruptBatch {
                declared: 1,
                actual: 2,
            },
        })),
        FetchOutcomeFailureClass::InvalidResponse
    );
}

fn decode(failure: FetchDecodeFailure) -> FetchOutcomeFailure {
    FetchOutcomeFailure::Response(FetchResponseFailure::Decode(failure))
}

fn classify(failure: &FetchOutcomeFailure) -> FetchOutcomeFailureClass {
    classify_fetch_outcome_failure(failure)
}
