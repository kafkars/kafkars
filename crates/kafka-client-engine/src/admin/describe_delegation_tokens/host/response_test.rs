//! Protocol failure classification and secret-bearing listing conversion.

use kafka_client_core::{DeliveryStatus, DescribeDelegationTokensInput};

use crate::protocol::admin::describe_delegation_tokens::{
    DescribeDelegationTokensResponseFailure, NormalizedDescribeDelegationTokenPrincipal,
    NormalizedDescribeDelegationTokensResponse, NormalizedDescribedDelegationToken,
};

use super::response::{normalized_input, protocol_failure};

#[test]
fn compatibility_capacity_and_malformed_shapes_remain_distinct() {
    assert_eq!(
        protocol_failure(DescribeDelegationTokensResponseFailure::MissingSelectedVersion),
        DescribeDelegationTokensInput::ProtocolIncompatible {
            delivery: DeliveryStatus::PossiblySent,
        }
    );
    assert_eq!(
        protocol_failure(DescribeDelegationTokensResponseFailure::RetainedBytes {
            required: 11,
            limit: 10,
        }),
        DescribeDelegationTokensInput::ResponseTooLarge
    );
    assert_eq!(
        protocol_failure(DescribeDelegationTokensResponseFailure::DuplicateToken),
        DescribeDelegationTokensInput::InvalidResponse
    );
}

#[test]
fn normalized_listing_moves_complete_tokens_into_core() {
    let normalized = NormalizedDescribeDelegationTokensResponse::fixture(
        7,
        0,
        vec![NormalizedDescribedDelegationToken::fixture(
            principal("User", "alice"),
            Some(principal("Service", "issuer")),
            10,
            20,
            30,
            "token-a".to_owned(),
            b"secret-a".to_vec(),
            vec![principal("Service", "renewer")],
        )],
        512,
    );

    let (input, retained) = normalized_input(normalized);
    let DescribeDelegationTokensInput::BrokerResponded { response } = input else {
        panic!("successful response expected");
    };
    let (throttle, tokens) = response.into_parts();
    let (owner, requester, renewers, issue, expiry, max, token_id, hmac) = tokens
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("token"))
        .into_parts();
    assert_eq!(throttle, 7);
    assert_eq!(owner.principal_name(), "alice");
    assert_eq!(
        requester.map(|value| value.into_parts()),
        Some(("Service".to_owned(), "issuer".to_owned()))
    );
    assert_eq!(renewers[0].principal_name(), "renewer");
    assert_eq!((issue, expiry, max), (10, 20, 30));
    assert_eq!(token_id, "token-a");
    assert_eq!(hmac.as_bytes(), b"secret-a");
    assert!(retained > 0);
}

fn principal(principal_type: &str, name: &str) -> NormalizedDescribeDelegationTokenPrincipal {
    NormalizedDescribeDelegationTokenPrincipal::fixture(principal_type.to_owned(), name.to_owned())
}
