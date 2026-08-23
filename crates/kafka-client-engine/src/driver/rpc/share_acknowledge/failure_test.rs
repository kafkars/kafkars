//! Driver error classification and delivery-independent failure evidence.

use kafka_driver::{CallFailure, Delivery, RequestError};
use kafka_wire_core::DecodeError;

use super::failure::{ShareAcknowledgeDriverFailureKind, classify_share_acknowledge_request_error};

#[test]
fn failures_preserve_deadline_protocol_and_transport_distinctions() {
    let cases = [
        (
            RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                delivery: Delivery::PossiblySent,
            },
            ShareAcknowledgeDriverFailureKind::DeadlineElapsed,
        ),
        (
            RequestError::Decode(DecodeError::UnexpectedEnd {
                offset: 1,
                needed: 4,
                remaining: 0,
            }),
            ShareAcknowledgeDriverFailureKind::InvalidResponse,
        ),
        (
            RequestError::RouteUnavailable,
            ShareAcknowledgeDriverFailureKind::Transport,
        ),
    ];
    for (error, expected) in cases {
        assert_eq!(classify_share_acknowledge_request_error(&error), expected);
    }
}
