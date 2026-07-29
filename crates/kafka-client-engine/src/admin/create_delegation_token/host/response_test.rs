//! Protocol failure classification and secret-bearing success conversion.

use kafka_client_core::{CreateDelegationTokenInput, DeliveryStatus};

use crate::protocol::admin::create_delegation_token::{
    CreateDelegationTokenResponseFailure, NormalizedCreateDelegationTokenResponse,
    NormalizedDelegationToken, NormalizedDelegationTokenPrincipal,
};

use super::response::{normalized_input, protocol_failure};

#[test]
fn compatibility_capacity_and_malformed_shapes_remain_distinct() {
    assert_eq!(
        protocol_failure(CreateDelegationTokenResponseFailure::MissingSelectedVersion),
        CreateDelegationTokenInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent,
        }
    );
    assert_eq!(
        protocol_failure(CreateDelegationTokenResponseFailure::RetainedBytes {
            required: 11,
            limit: 10,
        }),
        CreateDelegationTokenInput::ResponseTooLarge
    );
    assert_eq!(
        protocol_failure(CreateDelegationTokenResponseFailure::EmptyTokenId),
        CreateDelegationTokenInput::InvalidResponse
    );
}

#[test]
fn normalized_success_moves_complete_secret_bearing_response() {
    let normalized = NormalizedCreateDelegationTokenResponse::fixture(
        7,
        0,
        Some(NormalizedDelegationToken::fixture(
            principal("User", "owner"),
            Some(principal("User", "requester")),
            10,
            20,
            30,
            "token-id".to_owned(),
            b"secret-hmac".to_vec(),
        )),
        512,
    );

    let (input, retained) = normalized_input(normalized);
    let CreateDelegationTokenInput::BrokerResponded { response } = input else {
        panic!("successful response expected");
    };
    let (throttle, owner, requester, issue, expiry, max, token_id, hmac) = response.into_parts();
    assert_eq!(throttle, 7);
    assert_eq!(owner.principal_name(), "owner");
    assert_eq!(
        requester.map(|value| value.into_parts()),
        Some(("User".to_owned(), "requester".to_owned()))
    );
    assert_eq!((issue, expiry, max), (10, 20, 30));
    assert_eq!(token_id, "token-id");
    assert_eq!(hmac.as_bytes(), b"secret-hmac");
    assert!(retained > 0);
}

fn principal(principal_type: &str, name: &str) -> NormalizedDelegationTokenPrincipal {
    NormalizedDelegationTokenPrincipal::fixture(principal_type.to_owned(), name.to_owned())
}
