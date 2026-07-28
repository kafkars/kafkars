//! Admission, failure, completion, and host-fault facts.

use kafka_client_core::{
    DeliveryStatus, TransactionOffsetCommitId, TransactionOffsetCommitMachineError,
    TransactionOffsetCommitStage,
};

use crate::completion::CompletionObserver;

use super::input::TransactionOffsetCommitRequest;

/// One accepted operation and its sole reserved terminal observer.
#[must_use = "accepted transactional offsets require terminal observation or transfer"]
pub(crate) struct TransactionOffsetCommitAccepted {
    operation_id: TransactionOffsetCommitId,
    observer: CompletionObserver<TransactionOffsetCommitResult>,
}

impl TransactionOffsetCommitAccepted {
    pub(in crate::transaction) const fn new(
        operation_id: TransactionOffsetCommitId,
        observer: CompletionObserver<TransactionOffsetCommitResult>,
    ) -> Self {
        Self {
            operation_id,
            observer,
        }
    }

    pub(crate) const fn operation_id(&self) -> TransactionOffsetCommitId {
        self.operation_id
    }

    pub(crate) fn into_observer(self) -> CompletionObserver<TransactionOffsetCommitResult> {
        self.observer
    }
}

/// Definitely-unsent admission failure retaining the exact caller-owned input.
pub(crate) struct TransactionOffsetCommitAdmissionError {
    kind: TransactionOffsetCommitAdmissionErrorKind,
    input: TransactionOffsetCommitRequest,
}

impl TransactionOffsetCommitAdmissionError {
    pub(in crate::transaction) const fn new(
        kind: TransactionOffsetCommitAdmissionErrorKind,
        input: TransactionOffsetCommitRequest,
    ) -> Self {
        Self { kind, input }
    }

    pub(crate) const fn kind(&self) -> TransactionOffsetCommitAdmissionErrorKind {
        self.kind
    }

    pub(crate) fn into_input(self) -> TransactionOffsetCommitRequest {
        self.input
    }
}

/// Closed local reason for rejecting offset-transfer admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionOffsetCommitAdmissionErrorKind {
    Busy,
    CompletionCapacity,
    StaleOwner,
    InvalidLifecycle,
    InvalidInput,
    OffsetCount { actual: usize, limit: usize },
    RetainedBytes { actual: usize, limit: usize },
    IdentityExhausted,
}

/// Stable failure category for one request stage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionOffsetCommitFailureKind {
    DeadlineElapsed,
    DriverRejected,
    Allocation,
    Compatibility,
    InvalidResponse,
    Transport,
    Broker { code: i16, fenced: bool },
    Correlation,
    DriverShutdown,
}

/// Exact terminal failure and driver-authoritative delivery certainty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TransactionOffsetCommitFailure {
    kind: TransactionOffsetCommitFailureKind,
    delivery: DeliveryStatus,
}

impl TransactionOffsetCommitFailure {
    pub(in crate::transaction) const fn new(
        kind: TransactionOffsetCommitFailureKind,
        delivery: DeliveryStatus,
    ) -> Self {
        Self { kind, delivery }
    }

    pub(crate) const fn kind(self) -> TransactionOffsetCommitFailureKind {
        self.kind
    }

    pub(crate) const fn delivery(self) -> DeliveryStatus {
        self.delivery
    }
}

/// Health result of one terminally settled transfer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionOffsetCommitOutcome {
    Succeeded,
    RejectedNotSent {
        stage: TransactionOffsetCommitStage,
        failure: TransactionOffsetCommitFailure,
    },
    AbortRequired {
        stage: TransactionOffsetCommitStage,
        failure: TransactionOffsetCommitFailure,
    },
    Fatal {
        stage: TransactionOffsetCommitStage,
        failure: TransactionOffsetCommitFailure,
    },
}

/// Sole published terminal retaining the exact accepted input.
pub(crate) struct TransactionOffsetCommitResult {
    operation_id: TransactionOffsetCommitId,
    input: TransactionOffsetCommitRequest,
    outcome: TransactionOffsetCommitOutcome,
}

impl TransactionOffsetCommitResult {
    pub(super) const fn new(
        operation_id: TransactionOffsetCommitId,
        input: TransactionOffsetCommitRequest,
        outcome: TransactionOffsetCommitOutcome,
    ) -> Self {
        Self {
            operation_id,
            input,
            outcome,
        }
    }

    pub(crate) const fn operation_id(&self) -> TransactionOffsetCommitId {
        self.operation_id
    }

    pub(crate) const fn outcome(&self) -> TransactionOffsetCommitOutcome {
        self.outcome
    }

    pub(crate) fn into_input(self) -> TransactionOffsetCommitRequest {
        self.input
    }
}

/// Internal invariant or completion fault.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionOffsetCommitHostError {
    Completion(crate::completion::CompletionRegistryError),
    Core(TransactionOffsetCommitMachineError),
    Lifecycle,
    UnexpectedEffect,
}
