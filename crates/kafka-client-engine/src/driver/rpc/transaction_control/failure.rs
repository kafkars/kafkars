//! Stable normalization of transaction-control driver failures.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{CallFailure, RequestError};

use super::super::super::request_failure_delivery;

/// Stable failure kind without driver error vocabulary leakage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionControlDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
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
