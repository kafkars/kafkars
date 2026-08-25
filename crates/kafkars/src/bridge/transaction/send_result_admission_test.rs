//! Exhaustive transactional-send admission translation.

use kafka_client_engine::{TransactionControlErrorKind, TransactionSendAdmissionErrorKind};

use super::send_result::translate_send_admission;
use crate::{DeliveryStatus, ErrorKind, RetryAdvice};

type AdmissionCase = (TransactionSendAdmissionErrorKind, ErrorKind, RetryAdvice);

#[test]
fn every_send_admission_kind_has_one_stable_facade_category() {
    assert_send_admission_cases(validation_admission_cases());
    assert_send_admission_cases(capacity_admission_cases());
    assert_send_admission_cases(state_admission_cases());
}

fn validation_admission_cases() -> [AdmissionCase; 10] {
    use TransactionSendAdmissionErrorKind as Kind;
    [
        case(
            Kind::InvalidDeadline,
            ErrorKind::Timeout,
            RetryAdvice::DoNotRetry,
        ),
        case(
            Kind::EmptyBatch,
            ErrorKind::InvalidRecord,
            RetryAdvice::DoNotRetry,
        ),
        case(
            Kind::EmptyTopic,
            ErrorKind::InvalidRecord,
            RetryAdvice::DoNotRetry,
        ),
        case(
            Kind::NegativeExplicitPartition,
            ErrorKind::InvalidRecord,
            RetryAdvice::DoNotRetry,
        ),
        case(
            Kind::MissingExplicitPartition,
            ErrorKind::InvalidRecord,
            RetryAdvice::DoNotRetry,
        ),
        case(
            Kind::MixedBatchTopic,
            ErrorKind::InvalidRecord,
            RetryAdvice::DoNotRetry,
        ),
        case(
            Kind::MixedBatchTopicIdentity,
            ErrorKind::Identity,
            RetryAdvice::DoNotRetry,
        ),
        case(
            Kind::MixedBatchPartition,
            ErrorKind::InvalidRecord,
            RetryAdvice::DoNotRetry,
        ),
        case(
            Kind::RetainedSizeOverflow,
            ErrorKind::InvalidRecord,
            RetryAdvice::DoNotRetry,
        ),
        case(
            Kind::InvalidPartition,
            ErrorKind::InvalidRecord,
            RetryAdvice::DoNotRetry,
        ),
    ]
}

fn capacity_admission_cases() -> [AdmissionCase; 7] {
    use TransactionSendAdmissionErrorKind as Kind;
    [
        case(
            Kind::BatchRecordCapacity {
                actual: 2,
                limit: 1,
            },
            ErrorKind::Backpressure,
            RetryAdvice::DoNotRetry,
        ),
        case(
            Kind::Contended,
            ErrorKind::Backpressure,
            RetryAdvice::RetrySafe,
        ),
        case(
            Kind::RetainedRecordBytes {
                actual: 2,
                limit: 1,
            },
            ErrorKind::Backpressure,
            RetryAdvice::DoNotRetry,
        ),
        case(
            Kind::RetainedTopicCapacity {
                actual: 2,
                limit: 1,
            },
            ErrorKind::Backpressure,
            RetryAdvice::DoNotRetry,
        ),
        case(
            Kind::RetainedTopicBytes {
                actual: 2,
                limit: 1,
            },
            ErrorKind::Backpressure,
            RetryAdvice::DoNotRetry,
        ),
        case(
            Kind::Allocation,
            ErrorKind::Backpressure,
            RetryAdvice::DoNotRetry,
        ),
        case(Kind::Busy, ErrorKind::Backpressure, RetryAdvice::DoNotRetry),
    ]
}

fn state_admission_cases() -> [AdmissionCase; 7] {
    use TransactionSendAdmissionErrorKind as Kind;
    [
        case(
            Kind::TimestampUnavailable,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
        case(Kind::Closed, ErrorKind::State, RetryAdvice::DoNotRetry),
        case(Kind::StaleOwner, ErrorKind::State, RetryAdvice::DoNotRetry),
        case(
            Kind::RetainedTopicBytesOverflow,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
        case(
            Kind::TopicIdentityExhausted,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
        case(
            Kind::SendIdentityExhausted,
            ErrorKind::Internal,
            RetryAdvice::DoNotRetry,
        ),
        case(
            Kind::Transaction(TransactionControlErrorKind::Fenced),
            ErrorKind::State,
            RetryAdvice::DoNotRetry,
        ),
    ]
}

const fn case(
    kind: TransactionSendAdmissionErrorKind,
    error: ErrorKind,
    retry: RetryAdvice,
) -> AdmissionCase {
    (kind, error, retry)
}

fn assert_send_admission_cases<const N: usize>(cases: [AdmissionCase; N]) {
    for (kind, expected, expected_retry) in cases {
        let error = translate_send_admission(kind);
        assert_eq!(error.kind(), expected);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
        assert_eq!(error.retry_advice(), expected_retry, "{kind:?}");
        assert!(!error.requires_transaction_abort());
    }
}
