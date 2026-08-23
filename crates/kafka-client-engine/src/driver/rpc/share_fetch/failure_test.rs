//! Driver error classification and delivery-independent failure evidence.

use kafka_driver::{CallFailure, Delivery, RequestError};
use kafka_wire_core::DecodeError;

use super::failure::{ShareFetchDriverFailureKind, classify_share_fetch_request_error};

#[test]
fn driver_failures_preserve_deadline_protocol_and_transport_distinctions() {
    let cases = [
        (
            RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                delivery: Delivery::PossiblySent,
            },
            ShareFetchDriverFailureKind::DeadlineElapsed,
        ),
        (
            RequestError::Decode(DecodeError::UnexpectedEnd {
                offset: 1,
                needed: 4,
                remaining: 0,
            }),
            ShareFetchDriverFailureKind::InvalidResponse,
        ),
        (
            RequestError::RouteUnavailable,
            ShareFetchDriverFailureKind::Transport,
        ),
    ];
    for (error, expected) in cases {
        assert_eq!(classify_share_fetch_request_error(&error), expected);
    }
}
