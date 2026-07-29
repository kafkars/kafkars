//! Exact selected version and driver-authoritative failure scenarios.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiKey, ApiVersion, CallFailure, Delivery, RequestError};
use kafka_wire::DeleteShareGroupOffsetsResponse;
use kafka_wire_core::DecodeError;

use super::delete_share_group_offsets_terminal::{
    DeleteShareGroupOffsetsDriverFailureKind, DeleteShareGroupOffsetsTerminalFact,
    RecoveredDeleteShareGroupOffsetsCall, retain_delete_share_group_offsets_terminal,
};

#[test]
fn response_fact_borrows_exact_v0_and_generated_response() {
    let mut response = DeleteShareGroupOffsetsResponse::default();
    response.throttle_time_ms = 23;
    let terminal =
        retain_delete_share_group_offsets_terminal(Some(ApiVersion::new(0)), Ok(response), None);
    let DeleteShareGroupOffsetsTerminalFact::Response {
        selected_version,
        response,
    } = terminal.fact()
    else {
        panic!("response expected");
    };
    assert_eq!(selected_version, Some(0));
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
            DeleteShareGroupOffsetsDriverFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::VersionFloorUnavailable {
                api_key: ApiKey::new(92),
                minimum: ApiVersion::new(0),
                negotiated_maximum: ApiVersion::new(-1),
            },
            DeleteShareGroupOffsetsDriverFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            RequestError::Decode(DecodeError::UnexpectedEnd {
                offset: 1,
                needed: 4,
                remaining: 0,
            }),
            DeleteShareGroupOffsetsDriverFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::RouteUnavailable,
            DeleteShareGroupOffsetsDriverFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
    ];
    for (error, expected_kind, expected_delivery) in cases {
        let terminal = retain_delete_share_group_offsets_terminal(None, Err(error), None);
        let DeleteShareGroupOffsetsTerminalFact::Failed { kind, delivery } = terminal.fact() else {
            panic!("failure expected");
        };
        assert_eq!(kind, expected_kind);
        assert_eq!(delivery, expected_delivery);
        terminal.discard();
    }
}

#[test]
fn shutdown_recovery_token_seals_linearly() {
    RecoveredDeleteShareGroupOffsetsCall::new().seal();
}
