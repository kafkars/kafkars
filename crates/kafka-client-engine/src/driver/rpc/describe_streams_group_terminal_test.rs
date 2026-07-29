//! Exact selected versions and driver-authoritative failure scenarios.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiKey, ApiVersion, CallFailure, Delivery, RequestError};
use kafka_wire::StreamsGroupDescribeResponse;
use kafka_wire_core::DecodeError;

use super::describe_streams_group_terminal::{
    DescribeStreamsGroupDriverFailureKind, DescribeStreamsGroupTerminalFact,
    RecoveredDescribeStreamsGroupCall, retain_describe_streams_group_terminal,
};

#[test]
fn response_fact_borrows_selected_stable_version_and_generated_response() {
    for version in [0, 1] {
        let mut response = StreamsGroupDescribeResponse::default();
        response.throttle_time_ms = 23;
        let terminal = retain_describe_streams_group_terminal(
            Some(ApiVersion::new(version)),
            Ok(response),
            None,
        );
        let DescribeStreamsGroupTerminalFact::Response {
            selected_version,
            response,
        } = terminal.fact()
        else {
            panic!("response expected");
        };
        assert_eq!(selected_version, Some(version));
        assert_eq!(response.throttle_time_ms, 23);
        terminal.discard();
    }
}

#[test]
fn failures_preserve_delivery_certainty_and_stable_classification() {
    let cases = [
        (
            RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                delivery: Delivery::PossiblySent,
            },
            DescribeStreamsGroupDriverFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::VersionFloorUnavailable {
                api_key: ApiKey::new(89),
                minimum: ApiVersion::new(1),
                negotiated_maximum: ApiVersion::new(0),
            },
            DescribeStreamsGroupDriverFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            RequestError::Decode(DecodeError::UnexpectedEnd {
                offset: 1,
                needed: 4,
                remaining: 0,
            }),
            DescribeStreamsGroupDriverFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::RouteUnavailable,
            DescribeStreamsGroupDriverFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
    ];
    for (error, expected_kind, expected_delivery) in cases {
        let terminal = retain_describe_streams_group_terminal(None, Err(error), None);
        let DescribeStreamsGroupTerminalFact::Failed { kind, delivery } = terminal.fact() else {
            panic!("failure expected");
        };
        assert_eq!(kind, expected_kind);
        assert_eq!(delivery, expected_delivery);
        terminal.discard();
    }
}

#[test]
fn shutdown_recovery_token_seals_linearly() {
    RecoveredDescribeStreamsGroupCall::new().seal();
}
