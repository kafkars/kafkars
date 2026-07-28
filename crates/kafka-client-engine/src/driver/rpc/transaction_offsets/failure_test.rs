//! Delivery-preserving transaction-offset driver failure scenarios.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiKey, ApiVersion, CallFailure, Delivery, RequestError};
use kafka_wire_core::DecodeError;

use super::{
    TransactionOffsetDriverFailureKind,
    failure::{selected_version_failure, transaction_offset_driver_failure},
};

#[test]
fn failures_preserve_driver_delivery_and_stable_categories() {
    let cases = [
        (
            RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                delivery: Delivery::PossiblySent,
            },
            TransactionOffsetDriverFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::VersionFloorUnavailable {
                api_key: ApiKey::new(25),
                minimum: ApiVersion::new(4),
                negotiated_maximum: ApiVersion::new(3),
            },
            TransactionOffsetDriverFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            RequestError::Decode(DecodeError::UnexpectedEnd {
                offset: 1,
                needed: 4,
                remaining: 0,
            }),
            TransactionOffsetDriverFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::RouteUnavailable,
            TransactionOffsetDriverFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
    ];
    for (error, expected_kind, expected_delivery) in cases {
        assert_eq!(
            transaction_offset_driver_failure(&error),
            (expected_kind, expected_delivery)
        );
    }
}

#[test]
fn selected_version_failures_distinguish_missing_from_wrong() {
    assert_eq!(
        selected_version_failure(None),
        TransactionOffsetDriverFailureKind::InvalidResponse
    );
    assert_eq!(
        selected_version_failure(Some(3)),
        TransactionOffsetDriverFailureKind::Compatibility
    );
}
