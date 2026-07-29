//! API-key 38 v1-v3 request preparation and bounded token normalization.

mod model;
mod prepared;
mod request;
mod response;
mod retention;
mod secret;
mod validation;

#[cfg(test)]
mod fixture;

pub(crate) use model::{
    CreateDelegationTokenRequestRef, DelegationTokenPrincipalRef,
    NormalizedCreateDelegationTokenResponse, NormalizedDelegationToken,
    NormalizedDelegationTokenPrincipal,
};
pub(crate) use prepared::PreparedCreateDelegationTokenRequest;
pub(crate) use request::{CreateDelegationTokenRequestFailure, create_delegation_token_request};
pub(crate) use response::normalize_create_delegation_token_response;
#[cfg(test)]
pub(crate) use retention::CREATE_DELEGATION_TOKEN_MAX_RETAINED_BYTES;
pub(crate) use secret::DelegationTokenHmac;
pub(crate) use validation::CreateDelegationTokenResponseFailure;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
