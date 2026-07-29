//! Selected-version, response ownership, and driver-failure classification.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiKey, ApiVersion, CallFailure, Delivery, RequestError};
use kafka_wire::ExpireDelegationTokenResponse;
use kafka_wire_core::DecodeError;

use super::expire_delegation_token_terminal::{
    ExpireDelegationTokenDriverFailureKind, ExpireDelegationTokenTerminalFact,
    RecoveredExpireDelegationTokenCall, retain_expire_delegation_token_terminal,
};

#[test]
fn response_fact_borrows_exact_selected_version_and_generated_response() {
    for version in [1, 2] {
        let mut response = ExpireDelegationTokenResponse::default();
        response.expiry_timestamp_ms = 1_700_003_600_002;
        let terminal = retain_expire_delegation_token_terminal(
            Some(ApiVersion::new(version)),
            Ok(response),
            None,
        );
        let ExpireDelegationTokenTerminalFact::Response {
            selected_version,
            response,
        } = terminal.fact()
        else {
            panic!("response expected");
        };
        assert_eq!(selected_version, Some(version));
        assert_eq!(response.expiry_timestamp_ms, 1_700_003_600_002);
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
            ExpireDelegationTokenDriverFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::VersionFloorUnavailable {
                api_key: ApiKey::new(40),
                minimum: ApiVersion::new(1),
                negotiated_maximum: ApiVersion::new(0),
            },
            ExpireDelegationTokenDriverFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            RequestError::Decode(DecodeError::UnexpectedEnd {
                offset: 1,
                needed: 4,
                remaining: 0,
            }),
            ExpireDelegationTokenDriverFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::RouteUnavailable,
            ExpireDelegationTokenDriverFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
    ];
    for (error, expected_kind, expected_delivery) in cases {
        let terminal = retain_expire_delegation_token_terminal(None, Err(error), None);
        let ExpireDelegationTokenTerminalFact::Failed { kind, delivery } = terminal.fact() else {
            panic!("failure expected");
        };
        assert_eq!(kind, expected_kind);
        assert_eq!(delivery, expected_delivery);
        terminal.discard();
    }
}

#[test]
fn shutdown_recovery_token_seals_linearly() {
    RecoveredExpireDelegationTokenCall.seal();
}
