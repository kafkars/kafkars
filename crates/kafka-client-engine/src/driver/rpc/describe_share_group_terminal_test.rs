//! Exact selected version and driver-authoritative failure scenarios.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiKey, ApiVersion, CallFailure, Delivery, RequestError};
use kafka_wire::ShareGroupDescribeResponse;
use kafka_wire_core::DecodeError;

use super::describe_share_group_terminal::{
    DescribeShareGroupDriverFailureKind, DescribeShareGroupTerminalFact,
    RecoveredDescribeShareGroupCall, retain_describe_share_group_terminal,
};

#[test]
fn response_fact_borrows_exact_v1_and_generated_response() {
    let mut response = ShareGroupDescribeResponse::default();
    response.throttle_time_ms = 23;
    let terminal =
        retain_describe_share_group_terminal(Some(ApiVersion::new(1)), Ok(response), None);
    let DescribeShareGroupTerminalFact::Response {
        selected_version,
        response,
    } = terminal.fact()
    else {
        panic!("response expected");
    };
    assert_eq!(selected_version, Some(1));
    assert_eq!(response.throttle_time_ms, 23);
    terminal.discard();
}

#[test]
fn failures_preserve_delivery_certainty_and_stable_classification() {
    let cases = [
        (
            RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                delivery: Delivery::PossiblySent,
            },
            DescribeShareGroupDriverFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::VersionFloorUnavailable {
                api_key: ApiKey::new(77),
                minimum: ApiVersion::new(1),
                negotiated_maximum: ApiVersion::new(0),
            },
            DescribeShareGroupDriverFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            RequestError::Decode(DecodeError::UnexpectedEnd {
                offset: 1,
                needed: 4,
                remaining: 0,
            }),
            DescribeShareGroupDriverFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::RouteUnavailable,
            DescribeShareGroupDriverFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
    ];
    for (error, expected_kind, expected_delivery) in cases {
        let terminal = retain_describe_share_group_terminal(None, Err(error), None);
        let DescribeShareGroupTerminalFact::Failed { kind, delivery } = terminal.fact() else {
            panic!("failure expected");
        };
        assert_eq!(kind, expected_kind);
        assert_eq!(delivery, expected_delivery);
        terminal.discard();
    }
}

#[test]
fn shutdown_recovery_token_seals_linearly() {
    RecoveredDescribeShareGroupCall::new().seal();
}
