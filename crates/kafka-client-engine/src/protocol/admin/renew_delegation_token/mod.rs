//! API-key 39 v1-v2 request ownership and scalar response normalization.

mod model;
mod prepared;
mod request;
mod response;
mod retention;
mod secret;

pub(crate) use model::{NormalizedRenewDelegationTokenResponse, RenewDelegationTokenRequestRef};
pub(crate) use prepared::PreparedRenewDelegationTokenRequest;
pub(crate) use request::{RenewDelegationTokenRequestFailure, renew_delegation_token_request};
pub(crate) use response::{
    RenewDelegationTokenResponseFailure, normalize_renew_delegation_token_response,
};
#[cfg(test)]
pub(crate) use retention::RENEW_DELEGATION_TOKEN_MAX_RETAINED_BYTES;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
