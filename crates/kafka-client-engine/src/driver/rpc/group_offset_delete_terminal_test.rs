//! Selected-version and driver-authoritative failure classification scenarios.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiKey, ApiVersion, CallFailure, Delivery, RequestError};
use kafka_wire::OffsetDeleteResponse;
use kafka_wire_core::DecodeError;

use super::group_offset_delete_terminal::{
    GroupOffsetDeleteDriverFailureKind, GroupOffsetDeleteTerminalFact,
    RecoveredGroupOffsetDeleteCall, retain_group_offset_delete_terminal,
};

#[test]
fn response_fact_borrows_exact_selected_version_and_generated_response() {
    let mut response = OffsetDeleteResponse::default();
    response.throttle_time_ms = 19;
    let terminal =
        retain_group_offset_delete_terminal(Some(ApiVersion::new(0)), Ok(response), None);
    let GroupOffsetDeleteTerminalFact::Response {
        selected_version,
        response,
    } = terminal.fact()
    else {
        panic!("generated response expected");
    };
    assert_eq!(selected_version, Some(0));
    assert_eq!(response.throttle_time_ms, 19);
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
            GroupOffsetDeleteDriverFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::VersionFloorUnavailable {
                api_key: ApiKey::new(47),
                minimum: ApiVersion::new(1),
                negotiated_maximum: ApiVersion::new(0),
            },
            GroupOffsetDeleteDriverFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            RequestError::Decode(DecodeError::UnexpectedEnd {
                offset: 1,
                needed: 4,
                remaining: 0,
            }),
            GroupOffsetDeleteDriverFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::RouteUnavailable,
            GroupOffsetDeleteDriverFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
    ];
    for (error, expected_kind, expected_delivery) in cases {
        let terminal = retain_group_offset_delete_terminal(None, Err(error), None);
        let GroupOffsetDeleteTerminalFact::Failed { kind, delivery } = terminal.fact() else {
            panic!("failure fact expected");
        };
        assert_eq!(kind, expected_kind);
        assert_eq!(delivery, expected_delivery);
        terminal.discard();
    }
}

#[test]
fn shutdown_recovery_token_seals_linearly() {
    RecoveredGroupOffsetDeleteCall::new().seal();
}
