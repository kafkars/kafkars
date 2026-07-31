//! Classification scenarios for definitely-unsent Fetch admission failures.

use kafka_client_core::FetchFailure;

use crate::protocol::fetch::FetchRequestFailure;

use super::{admission::FetchAdmissionFailureSource, failure::classify_fetch_admission};

#[test]
fn elapsed_admission_preserves_deadline_precedence() {
    assert_eq!(
        classify_fetch_admission(&FetchAdmissionFailureSource::DeadlineElapsed),
        FetchFailure::DeadlineElapsed
    );
}

#[test]
fn every_closed_request_construction_failure_is_driver_rejected() {
    let failures = [
        FetchRequestFailure::Allocation,
        FetchRequestFailure::EmptyTopic,
        FetchRequestFailure::TopicTooLong {
            actual: 250,
            limit: 249,
        },
        FetchRequestFailure::PartitionOutOfRange { actual: u32::MAX },
        FetchRequestFailure::NegativeFetchOffset { actual: -1 },
        FetchRequestFailure::MaxWaitOutOfRange { actual: u32::MAX },
        FetchRequestFailure::MinBytesOutOfRange { actual: u32::MAX },
        FetchRequestFailure::MaxBytesOutOfRange { actual: 0 },
        FetchRequestFailure::PartitionMaxBytesOutOfRange { actual: 0 },
        FetchRequestFailure::MinBytesExceedMaxBytes {
            min_bytes: 2,
            max_bytes: 1,
        },
        FetchRequestFailure::InvalidIsolationLevel { actual: 2 },
    ];
    for failure in failures {
        assert_eq!(
            classify_fetch_admission(&FetchAdmissionFailureSource::Request(failure)),
            FetchFailure::DriverRejected
        );
    }
}
