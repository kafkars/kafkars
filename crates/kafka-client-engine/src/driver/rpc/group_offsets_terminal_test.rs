//! Neutral selected-version and driver-failure classification scenarios.

use kafka_client_core::{DeliveryStatus, ListConsumerGroupOffsetsPlan};
use kafka_driver::{ApiKey, ApiVersion, CallFailure, Delivery, RequestError};
use kafka_wire::OffsetFetchResponse;
use kafka_wire_core::DecodeError;

use super::group_offsets_call::GroupOffsetsEvidence;
use super::group_offsets_terminal::{
    GroupOffsetsDriverFailureKind, GroupOffsetsTerminalFact, retain_group_offsets_terminal,
};

#[test]
fn response_fact_borrows_exact_selected_version_and_generated_response() {
    let mut response = OffsetFetchResponse::default();
    response.throttle_time_ms = 19;
    let terminal =
        retain_group_offsets_terminal(Some(ApiVersion::new(9)), Ok(response), None, evidence());
    let GroupOffsetsTerminalFact::Response {
        selected_version,
        response,
    } = terminal.fact()
    else {
        panic!("generated response expected");
    };
    assert_eq!(selected_version, Some(9));
    assert_eq!(response.throttle_time_ms, 19);
    terminal.discard();
}

#[test]
fn failures_are_neutral_and_preserve_driver_authoritative_delivery() {
    let cases = [
        (
            RequestError::Rejected {
                failure: CallFailure::DeadlineExceeded,
                delivery: Delivery::PossiblySent,
            },
            GroupOffsetsDriverFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::VersionFloorUnavailable {
                api_key: ApiKey::new(9),
                minimum: ApiVersion::new(7),
                negotiated_maximum: ApiVersion::new(6),
            },
            GroupOffsetsDriverFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            RequestError::Decode(DecodeError::UnexpectedEnd {
                offset: 1,
                needed: 4,
                remaining: 0,
            }),
            GroupOffsetsDriverFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::RouteUnavailable,
            GroupOffsetsDriverFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
    ];
    for (error, expected_kind, expected_delivery) in cases {
        let terminal = retain_group_offsets_terminal(None, Err(error), None, evidence());
        let GroupOffsetsTerminalFact::Failed { kind, delivery } = terminal.fact() else {
            panic!("failure fact expected");
        };
        assert_eq!(kind, expected_kind);
        assert_eq!(delivery, expected_delivery);
        terminal.discard();
    }
}

fn evidence() -> GroupOffsetsEvidence {
    GroupOffsetsEvidence::new(
        ListConsumerGroupOffsetsPlan::new("readers".to_owned(), true)
            .unwrap_or_else(|error| panic!("valid plan: {error}")),
        4096,
    )
}
