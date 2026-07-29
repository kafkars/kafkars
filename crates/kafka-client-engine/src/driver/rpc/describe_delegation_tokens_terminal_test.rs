//! Selected-version, response ownership, and driver-failure classification.

use kafka_client_core::{
    DelegationTokenPrincipal, DeliveryStatus, DescribeDelegationTokensPlan,
    DescribeDelegationTokensSelection,
};
use kafka_driver::{ApiKey, ApiVersion, CallFailure, Delivery, RequestError};
use kafka_wire::DescribeDelegationTokenResponse;
use kafka_wire_core::DecodeError;

use super::describe_delegation_tokens_terminal::{
    DescribeDelegationTokensDriverFailureKind, DescribeDelegationTokensTerminalFact,
    RecoveredDescribeDelegationTokensCall, retain_describe_delegation_tokens_terminal,
};

#[test]
fn response_fact_borrows_exact_selected_version_and_generated_response() {
    for version in [1, 2, 3] {
        let mut response = DescribeDelegationTokenResponse::default();
        response.throttle_time_ms = 23;
        let terminal = retain_describe_delegation_tokens_terminal(
            Some(ApiVersion::new(version)),
            Ok(response),
            None,
        );
        let DescribeDelegationTokensTerminalFact::Response {
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
            DescribeDelegationTokensDriverFailureKind::DeadlineElapsed,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::VersionFloorUnavailable {
                api_key: ApiKey::new(41),
                minimum: ApiVersion::new(1),
                negotiated_maximum: ApiVersion::new(0),
            },
            DescribeDelegationTokensDriverFailureKind::Compatibility,
            DeliveryStatus::NotSent,
        ),
        (
            RequestError::Decode(DecodeError::UnexpectedEnd {
                offset: 1,
                needed: 4,
                remaining: 0,
            }),
            DescribeDelegationTokensDriverFailureKind::InvalidResponse,
            DeliveryStatus::PossiblySent,
        ),
        (
            RequestError::RouteUnavailable,
            DescribeDelegationTokensDriverFailureKind::Transport,
            DeliveryStatus::NotSent,
        ),
    ];
    for (error, expected_kind, expected_delivery) in cases {
        let terminal = retain_describe_delegation_tokens_terminal(None, Err(error), None);
        let DescribeDelegationTokensTerminalFact::Failed { kind, delivery } = terminal.fact()
        else {
            panic!("failure expected");
        };
        assert_eq!(kind, expected_kind);
        assert_eq!(delivery, expected_delivery);
        terminal.discard();
    }
}

#[test]
fn shutdown_recovery_token_seals_linearly() {
    let recovered = RecoveredDescribeDelegationTokensCall::for_test(owner_plan());
    let DescribeDelegationTokensSelection::Owners(owners) = recovered.plan().selection() else {
        panic!("explicit owners expected");
    };
    assert_eq!(owners[0].principal_name(), "alice");
    recovered.seal();
}

fn owner_plan() -> DescribeDelegationTokensPlan {
    DescribeDelegationTokensPlan::for_owners(vec![
        DelegationTokenPrincipal::new("User".to_owned(), "alice".to_owned())
            .unwrap_or_else(|error| panic!("valid owner: {error}")),
    ])
    .unwrap_or_else(|error| panic!("valid owner selection: {error}"))
}
