//! Topic-view adapter type and exact failure-domain smoke scenarios.

use kafka_driver::SubmitError;

use super::{
    TopicPartitionCountAdmissionFailure, TopicPartitionCountAdmissionFailureKind,
    TopicPartitionCountFact, TopicPartitionCountFailure,
};

#[test]
fn scalar_fact_retains_generation_and_total_logical_count() {
    let fact = TopicPartitionCountFact {
        metadata_generation: 11,
        logical_partition_count: 7,
    };

    assert_eq!(fact.metadata_generation, 11);
    assert_eq!(fact.logical_partition_count, 7);
}

#[test]
fn completion_fault_stays_distinct_from_metadata_unavailability() {
    assert_ne!(
        TopicPartitionCountFailure::Completion,
        TopicPartitionCountFailure::Unavailable
    );
}

#[test]
fn only_full_driver_admission_is_retryable_backpressure() {
    assert_eq!(
        TopicPartitionCountAdmissionFailure::Driver(SubmitError::Full).kind(),
        TopicPartitionCountAdmissionFailureKind::Full
    );
    for source in [
        SubmitError::Closed,
        SubmitError::Wake(std::io::Error::other("wake failed")),
        SubmitError::IdentityExhausted,
        SubmitError::ForeignDriver,
    ] {
        assert_eq!(
            TopicPartitionCountAdmissionFailure::Driver(source).kind(),
            TopicPartitionCountAdmissionFailureKind::Terminal
        );
    }
}
