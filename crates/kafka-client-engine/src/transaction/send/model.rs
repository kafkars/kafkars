//! Exact transactional send input, admission failure, and terminal facts.

use kafka_client_core::{
    DeliveryStatus, PartitionIndex, ProducerAttemptFailureKind, ProducerBatchSuccess,
    TransactionEpoch, TransactionSendId,
};

use super::partitioning::TransactionPartitioningFailure;
use crate::{
    completion::CompletionObserver, driver::transaction_produce::TransactionProduceFailureKind,
    transaction::partition_enrollment::TransactionPartitionEnrollmentFailureKind,
};

/// Fixed terminal reservation returned only after deterministic send acceptance.
#[must_use = "an accepted transactional send must retain or transfer its observer"]
#[derive(Debug)]
pub(crate) struct TransactionSendAccepted {
    send_id: TransactionSendId,
    observer: CompletionObserver<TransactionSendTerminal>,
}

impl TransactionSendAccepted {
    pub(super) const fn new(
        send_id: TransactionSendId,
        observer: CompletionObserver<TransactionSendTerminal>,
    ) -> Self {
        Self { send_id, observer }
    }

    pub(crate) const fn send_id(&self) -> TransactionSendId {
        self.send_id
    }

    pub(crate) fn into_observer(self) -> CompletionObserver<TransactionSendTerminal> {
        self.observer
    }
}

/// Stable failure category retained by the fixed terminal slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionSendFailureKind {
    Partitioning(TransactionPartitioningFailure),
    Enrollment(TransactionPartitionEnrollmentFailureKind),
    DeadlineElapsed,
    Materialization,
    ProduceSubmission(ProducerAttemptFailureKind),
    Produce(TransactionProduceFailureKind),
    InvalidResponse,
    Correlation,
    DriverShutdown,
}

/// Exact failure category and authoritative transport certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TransactionSendFailure {
    kind: TransactionSendFailureKind,
    delivery: DeliveryStatus,
}

impl TransactionSendFailure {
    pub(super) const fn new(kind: TransactionSendFailureKind, delivery: DeliveryStatus) -> Self {
        Self { kind, delivery }
    }

    pub(crate) const fn kind(self) -> TransactionSendFailureKind {
        self.kind
    }

    pub(crate) const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// One accepted transactional send terminal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionSendTerminal {
    Succeeded {
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        partition: PartitionIndex,
        success: ProducerBatchSuccess,
        last_offset: i64,
    },
    FailedHealthy {
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        failure: TransactionSendFailure,
    },
    AbortRequired {
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        failure: TransactionSendFailure,
    },
    Fatal {
        epoch: TransactionEpoch,
        send_id: TransactionSendId,
        failure: TransactionSendFailure,
    },
}

/// At-most-one action from a bounded send-owner turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionSendTurn {
    Idle,
    Progress,
}
