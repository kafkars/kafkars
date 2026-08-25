//! Stable normalization of transaction-control driver failures.

use std::{error::Error, fmt};

use kafka_client_core::DeliveryStatus;
use kafka_driver::{CallFailure, CompletionError, RequestError, SubmitError};

use super::submission::TransactionControlSubmitError;

use super::super::super::request_failure_delivery;

/// Stable failure kind without driver error vocabulary leakage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionControlDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

/// Stable reason the driver rejected an `EndTxn` call before transport ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionEndCallAdmissionFailureKind {
    InvalidTransactionalId,
    Capacity,
    Closed,
    Wake,
    Compatibility,
    ForeignDriver,
    DriverRejected,
}

/// Definitely-unsent coordinator-key or driver-admission failure.
#[derive(Debug)]
pub(crate) enum TransactionEndCallAdmissionFailure {
    Driver(TransactionControlSubmitError),
}

impl TransactionEndCallAdmissionFailure {
    pub(crate) const fn kind(&self) -> TransactionEndCallAdmissionFailureKind {
        match self {
            Self::Driver(error) => transaction_end_admission_failure(error),
        }
    }
}

impl fmt::Display for TransactionEndCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Driver(source) => source.fmt(formatter),
        }
    }
}

impl Error for TransactionEndCallAdmissionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Driver(source) => Some(source),
        }
    }
}

pub(super) const fn transaction_end_admission_failure(
    error: &TransactionControlSubmitError,
) -> TransactionEndCallAdmissionFailureKind {
    match error {
        TransactionControlSubmitError::InvalidTransactionalId(_) => {
            TransactionEndCallAdmissionFailureKind::InvalidTransactionalId
        }
        TransactionControlSubmitError::Driver(
            SubmitError::Full | SubmitError::IdentityExhausted,
        ) => TransactionEndCallAdmissionFailureKind::Capacity,
        TransactionControlSubmitError::Driver(SubmitError::Closed) => {
            TransactionEndCallAdmissionFailureKind::Closed
        }
        TransactionControlSubmitError::Driver(SubmitError::Wake(_)) => {
            TransactionEndCallAdmissionFailureKind::Wake
        }
        TransactionControlSubmitError::Driver(SubmitError::VersionBoundsInvalid { .. }) => {
            TransactionEndCallAdmissionFailureKind::Compatibility
        }
        TransactionControlSubmitError::Driver(SubmitError::ForeignDriver) => {
            TransactionEndCallAdmissionFailureKind::ForeignDriver
        }
        TransactionControlSubmitError::Driver(_) => {
            TransactionEndCallAdmissionFailureKind::DriverRejected
        }
    }
}

/// Stable reason observation of an accepted `EndTxn` call failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionEndCompletionFailureKind {
    Closed,
    Consumed,
    Correlation,
}

pub(super) const fn transaction_end_completion_failure(
    error: CompletionError,
) -> TransactionEndCompletionFailureKind {
    match error {
        CompletionError::Closed => TransactionEndCompletionFailureKind::Closed,
        CompletionError::Consumed => TransactionEndCompletionFailureKind::Consumed,
        _ => TransactionEndCompletionFailureKind::Correlation,
    }
}

pub(super) fn transaction_control_driver_failure(
    error: &RequestError,
) -> (TransactionControlDriverFailureKind, DeliveryStatus) {
    let kind = match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => TransactionControlDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => TransactionControlDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            TransactionControlDriverFailureKind::Compatibility
        }
        _ => TransactionControlDriverFailureKind::Transport,
    };
    (kind, request_failure_delivery(error))
}
