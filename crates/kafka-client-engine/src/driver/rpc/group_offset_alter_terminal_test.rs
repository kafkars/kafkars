//! Selected-version and driver-authoritative failure classification scenarios.

use kafka_client_core::{
    AlterConsumerGroupOffsetTarget, AlterConsumerGroupOffsetsPlan, DeliveryStatus,
};
use kafka_driver::{ApiKey, ApiVersion, CallFailure, Delivery, RequestError};
use kafka_wire::OffsetCommitResponse;
use kafka_wire_core::DecodeError;

use super::group_offset_alter_call::GroupOffsetAlterEvidence;
use super::group_offset_alter_terminal::{
    GroupOffsetAlterDriverFailureKind, GroupOffsetAlterTerminalFact, RecoveredGroupOffsetAlterCall,
    retain_group_offset_alter_terminal,
};

#[test]
fn response_fact_borrows_exact_selected_version_and_generated_response() {
    let mut response = OffsetCommitResponse::default();
    response.throttle_time_ms = 19;
    let terminal = retain_group_offset_alter_terminal(
        Some(ApiVersion::new(6)),
        Ok(response),
        None,
        evidence(),
    );
    let GroupOffsetAlterTerminalFact::Response {
        selected_version,
        response,
    } = terminal.fact()
    else {
        panic!("generated response expected");
    };
    assert_eq!(selected_version, Some(6));
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
            GroupOffsetAlterDriverFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::VersionFloorUnavailable {
                api_key: ApiKey::new(8),
                minimum: ApiVersion::new(6),
                negotiated_maximum: ApiVersion::new(5),
            },
            GroupOffsetAlterDriverFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            RequestError::Decode(DecodeError::UnexpectedEnd {
                offset: 1,
                needed: 4,
                remaining: 0,
            }),
            GroupOffsetAlterDriverFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::RouteUnavailable,
            GroupOffsetAlterDriverFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
    ];
    for (error, expected_kind, expected_delivery) in cases {
        let terminal = retain_group_offset_alter_terminal(None, Err(error), None, evidence());
        let GroupOffsetAlterTerminalFact::Failed { kind, delivery } = terminal.fact() else {
            panic!("failure fact expected");
        };
        assert_eq!(kind, expected_kind);
        assert_eq!(delivery, expected_delivery);
        terminal.discard();
    }
}

#[test]
fn shutdown_recovery_token_seals_linearly() {
    RecoveredGroupOffsetAlterCall::new(evidence()).seal();
}

fn evidence() -> GroupOffsetAlterEvidence {
    GroupOffsetAlterEvidence::new(plan(), 4_096, 8_192)
}

fn plan() -> AlterConsumerGroupOffsetsPlan {
    AlterConsumerGroupOffsetsPlan::new(
        "readers".to_owned(),
        vec![AlterConsumerGroupOffsetTarget::new(
            "orders".to_owned(),
            0,
            91,
            None,
            None,
        )],
    )
    .unwrap_or_else(|error| panic!("valid alteration plan: {error}"))
}
