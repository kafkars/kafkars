//! Exhaustive private-to-public transactional send failure translation.

use kafka_client_core::{DeliveryStatus, ProducerAttemptFailureKind};

use crate::{
    driver::transaction_produce::TransactionProduceFailureKind,
    transaction::{
        partition_enrollment::TransactionPartitionEnrollmentFailureKind,
        send::{InternalTransactionSendFailure, InternalTransactionSendFailureKind},
    },
};

use super::{
    TransactionSendConsequence, TransactionSendDeliveryStatus, TransactionSendFailure,
    TransactionSendFailureKind, TransactionSendOutcome,
};

pub(super) fn public_failure(
    failure: InternalTransactionSendFailure,
    consequence: TransactionSendConsequence,
) -> TransactionSendOutcome {
    let (kind, broker_code) = failure_kind(failure.kind());
    TransactionSendOutcome::Failed(TransactionSendFailure {
        kind,
        delivery: delivery(failure.delivery()),
        broker_code,
        consequence,
    })
}

fn failure_kind(
    kind: InternalTransactionSendFailureKind,
) -> (TransactionSendFailureKind, Option<i16>) {
    match kind {
        InternalTransactionSendFailureKind::Partitioning(kind) => match kind {
            crate::transaction::send::InternalTransactionPartitioningFailure::DeadlineElapsed => {
                (TransactionSendFailureKind::DeadlineElapsed, None)
            }
            crate::transaction::send::InternalTransactionPartitioningFailure::MetadataUnavailable {
                broker_code: Some(code),
            } => (TransactionSendFailureKind::Broker, Some(code)),
            crate::transaction::send::InternalTransactionPartitioningFailure::MetadataUnavailable {
                broker_code: None,
            } => (TransactionSendFailureKind::Routing, None),
            crate::transaction::send::InternalTransactionPartitioningFailure::Capacity => {
                (TransactionSendFailureKind::Backpressure, None)
            }
        },
        InternalTransactionSendFailureKind::Enrollment(kind) => enrollment_failure_kind(kind),
        InternalTransactionSendFailureKind::DeadlineElapsed => {
            (TransactionSendFailureKind::DeadlineElapsed, None)
        }
        InternalTransactionSendFailureKind::Materialization => {
            (TransactionSendFailureKind::Materialization, None)
        }
        InternalTransactionSendFailureKind::ProduceSubmission(kind) => attempt_failure_kind(kind),
        InternalTransactionSendFailureKind::Produce(kind) => produce_failure_kind(kind),
        InternalTransactionSendFailureKind::InvalidResponse => {
            (TransactionSendFailureKind::InvalidResponse, None)
        }
        InternalTransactionSendFailureKind::Correlation => {
            (TransactionSendFailureKind::Correlation, None)
        }
        InternalTransactionSendFailureKind::DriverShutdown => {
            (TransactionSendFailureKind::DriverClosed, None)
        }
    }
}

fn enrollment_failure_kind(
    kind: TransactionPartitionEnrollmentFailureKind,
) -> (TransactionSendFailureKind, Option<i16>) {
    match kind {
        TransactionPartitionEnrollmentFailureKind::Busy => (TransactionSendFailureKind::Busy, None),
        TransactionPartitionEnrollmentFailureKind::EpochMismatch => {
            (TransactionSendFailureKind::StaleTransaction, None)
        }
        TransactionPartitionEnrollmentFailureKind::OwnerMismatch => {
            (TransactionSendFailureKind::OwnerUnavailable, None)
        }
        TransactionPartitionEnrollmentFailureKind::InvalidTarget => {
            (TransactionSendFailureKind::InvalidTarget, None)
        }
        TransactionPartitionEnrollmentFailureKind::Capacity
        | TransactionPartitionEnrollmentFailureKind::RetainedBytes => {
            (TransactionSendFailureKind::Backpressure, None)
        }
        TransactionPartitionEnrollmentFailureKind::DeadlineElapsed => {
            (TransactionSendFailureKind::DeadlineElapsed, None)
        }
        TransactionPartitionEnrollmentFailureKind::DriverRejected => {
            (TransactionSendFailureKind::DriverRejected, None)
        }
        TransactionPartitionEnrollmentFailureKind::Transport => {
            (TransactionSendFailureKind::Transport, None)
        }
        TransactionPartitionEnrollmentFailureKind::Compatibility => {
            (TransactionSendFailureKind::Compatibility, None)
        }
        TransactionPartitionEnrollmentFailureKind::InvalidResponse => {
            (TransactionSendFailureKind::InvalidResponse, None)
        }
        TransactionPartitionEnrollmentFailureKind::DriverClosed => {
            (TransactionSendFailureKind::DriverClosed, None)
        }
        TransactionPartitionEnrollmentFailureKind::Broker { code, .. } => {
            (TransactionSendFailureKind::Broker, Some(code))
        }
    }
}

fn produce_failure_kind(
    kind: TransactionProduceFailureKind,
) -> (TransactionSendFailureKind, Option<i16>) {
    match kind {
        TransactionProduceFailureKind::Broker(failure) => {
            (TransactionSendFailureKind::Broker, Some(failure.code()))
        }
        TransactionProduceFailureKind::Protocol(_) => {
            (TransactionSendFailureKind::InvalidResponse, None)
        }
        TransactionProduceFailureKind::Driver(kind) => attempt_failure_kind(kind),
        TransactionProduceFailureKind::CompletionLost
        | TransactionProduceFailureKind::DriverShutdown => {
            (TransactionSendFailureKind::DriverClosed, None)
        }
    }
}

const fn attempt_failure_kind(
    kind: ProducerAttemptFailureKind,
) -> (TransactionSendFailureKind, Option<i16>) {
    let kind = match kind {
        ProducerAttemptFailureKind::LocalCapacity => TransactionSendFailureKind::Backpressure,
        ProducerAttemptFailureKind::RouteUnavailable => TransactionSendFailureKind::Routing,
        ProducerAttemptFailureKind::NameResolutionUnavailable => {
            TransactionSendFailureKind::NameResolution
        }
        ProducerAttemptFailureKind::ConnectionUnavailable => {
            TransactionSendFailureKind::ConnectionUnavailable
        }
        ProducerAttemptFailureKind::InvalidResponse => TransactionSendFailureKind::InvalidResponse,
        ProducerAttemptFailureKind::Compatibility => TransactionSendFailureKind::Compatibility,
        ProducerAttemptFailureKind::Permanent => TransactionSendFailureKind::Permanent,
    };
    (kind, None)
}

const fn delivery(delivery: DeliveryStatus) -> TransactionSendDeliveryStatus {
    match delivery {
        DeliveryStatus::NotSent => TransactionSendDeliveryStatus::NotSent,
        DeliveryStatus::PossiblySent => TransactionSendDeliveryStatus::PossiblySent,
    }
}
