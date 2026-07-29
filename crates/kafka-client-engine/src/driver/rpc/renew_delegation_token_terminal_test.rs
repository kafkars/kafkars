//! Selected-version, response ownership, and driver-failure classification.

use kafka_client_core::DeliveryStatus;
use kafka_driver::{ApiKey, ApiVersion, CallFailure, Delivery, RequestError};
use kafka_wire::RenewDelegationTokenResponse;
use kafka_wire_core::DecodeError;

use super::renew_delegation_token_terminal::{
    RecoveredRenewDelegationTokenCall, RenewDelegationTokenDriverFailureKind,
    RenewDelegationTokenTerminalFact, retain_renew_delegation_token_terminal,
};

#[test]
fn response_fact_borrows_exact_selected_version_and_generated_response() {
    for version in [1, 2] {
        let mut response = RenewDelegationTokenResponse::default();
        response.expiry_timestamp_ms = 1_700_003_600_002;
        let terminal = retain_renew_delegation_token_terminal(
            Some(ApiVersion::new(version)),
            Ok(response),
            None,
        );
        let RenewDelegationTokenTerminalFact::Response {
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
            RenewDelegationTokenDriverFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::VersionFloorUnavailable {
                api_key: ApiKey::new(39),
                minimum: ApiVersion::new(1),
                negotiated_maximum: ApiVersion::new(0),
            },
            RenewDelegationTokenDriverFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            RequestError::Decode(DecodeError::UnexpectedEnd {
                offset: 1,
                needed: 4,
                remaining: 0,
            }),
            RenewDelegationTokenDriverFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::RouteUnavailable,
            RenewDelegationTokenDriverFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
    ];
    for (error, expected_kind, expected_delivery) in cases {
        let terminal = retain_renew_delegation_token_terminal(None, Err(error), None);
        let RenewDelegationTokenTerminalFact::Failed { kind, delivery } = terminal.fact() else {
            panic!("failure expected");
        };
        assert_eq!(kind, expected_kind);
        assert_eq!(delivery, expected_delivery);
        terminal.discard();
    }
}

#[test]
fn shutdown_recovery_token_seals_linearly() {
    RecoveredRenewDelegationTokenCall.seal();
}
