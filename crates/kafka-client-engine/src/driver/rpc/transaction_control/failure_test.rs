//! Delivery-preserving transaction-control driver failure scenarios.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiKey, ApiVersion, CallFailure, Delivery, RequestError};
use kafka_wire_core::DecodeError;

use super::{TransactionControlDriverFailureKind, failure::transaction_control_driver_failure};

#[test]
fn failures_preserve_driver_delivery_and_stable_categories() {
    let cases = [
        (
            RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                delivery: Delivery::PossiblySent,
            },
            TransactionControlDriverFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::VersionFloorUnavailable {
                api_key: ApiKey::new(24),
                minimum: ApiVersion::new(3),
                negotiated_maximum: ApiVersion::new(2),
            },
            TransactionControlDriverFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            RequestError::Decode(DecodeError::UnexpectedEnd {
                offset: 1,
                needed: 4,
                remaining: 0,
            }),
            TransactionControlDriverFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::RouteUnavailable,
            TransactionControlDriverFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
    ];
    for (error, expected_kind, expected_delivery) in cases {
        assert_eq!(
            transaction_control_driver_failure(&error),
            (expected_kind, expected_delivery)
        );
    }
}
