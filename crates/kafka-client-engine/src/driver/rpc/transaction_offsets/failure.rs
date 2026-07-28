//! Delivery-preserving failure classification for transactional offset calls.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{CallFailure, RequestError};

use super::super::super::request_failure_delivery;

/// Stable failure kind without driver vocabulary leakage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionOffsetDriverFailureKind {
    DeadlineElapsed,
    Compatibility,
    InvalidResponse,
    Transport,
}

pub(super) fn transaction_offset_driver_failure(
    error: &RequestError,
) -> (TransactionOffsetDriverFailureKind, DeliveryStatus) {
    let kind = match error {
        RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        } => TransactionOffsetDriverFailureKind::DeadlineElapsed,
        RequestError::Rejected {
            failure: CallFailure::CorrelationMismatch { .. },
            ..
        }
        | RequestError::Decode(_) => TransactionOffsetDriverFailureKind::InvalidResponse,
        RequestError::Encode(_)
        | RequestError::UnsupportedVersion { .. }
        | RequestError::ApiUnavailable { .. }
        | RequestError::VersionLimitUnavailable { .. }
        | RequestError::VersionFloorUnavailable { .. }
        | RequestError::VersionBoundsInvalid { .. } => {
            TransactionOffsetDriverFailureKind::Compatibility
        }
        _ => TransactionOffsetDriverFailureKind::Transport,
    };
    (kind, request_failure_delivery(error))
}

pub(super) const fn selected_version_failure(
    selected_version: Option<i16>,
) -> TransactionOffsetDriverFailureKind {
    match selected_version {
        None => TransactionOffsetDriverFailureKind::InvalidResponse,
        Some(_) => TransactionOffsetDriverFailureKind::Compatibility,
    }
}
