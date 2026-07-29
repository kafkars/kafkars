//! Exact renewal success, broker rejection, and protocol-failure translation.

use kafka_client_core::RenewDelegationTokenInput;

use crate::protocol::admin::renew_delegation_token::{
    NormalizedRenewDelegationTokenResponse, RenewDelegationTokenResponseFailure,
};

use super::response::{normalized_input, protocol_failure};

#[test]
fn normalized_success_preserves_throttle_and_expiry() {
    let normalized =
        NormalizedRenewDelegationTokenResponse::fixture(17, 0, Some(1_700_003_600_002), 128);

    let (input, retained) = normalized_input(normalized);
    let RenewDelegationTokenInput::BrokerResponded { response } = input else {
        panic!("successful core input expected");
    };
    assert_eq!(response.into_parts(), (17, 1_700_003_600_002));
    assert!(retained > 0);
}

#[test]
fn normalized_rejection_preserves_exact_signed_code() {
    let normalized = NormalizedRenewDelegationTokenResponse::fixture(19, -31_234, None, 128);

    let (input, _) = normalized_input(normalized);
    let RenewDelegationTokenInput::BrokerRejected { error } = input else {
        panic!("exact broker rejection expected");
    };
    assert_eq!(error.into_parts(), (19, -31_234));
}

#[test]
fn compatibility_capacity_and_malformed_scalars_remain_distinct() {
    assert!(matches!(
        protocol_failure(RenewDelegationTokenResponseFailure::MissingSelectedVersion),
        RenewDelegationTokenInput::ProtocolIncompatible { .. }
    ));
    assert!(matches!(
        protocol_failure(RenewDelegationTokenResponseFailure::RetainedBytes {
            required: 2,
            limit: 1,
        }),
        RenewDelegationTokenInput::ResponseTooLarge
    ));
    assert!(matches!(
        protocol_failure(
            RenewDelegationTokenResponseFailure::InvalidExpiryTimestamp { actual: -1 }
        ),
        RenewDelegationTokenInput::InvalidResponse
    ));
}
