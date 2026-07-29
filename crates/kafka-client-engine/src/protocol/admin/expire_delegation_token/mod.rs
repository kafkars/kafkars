//! API-key 40 v1-v2 request ownership and scalar response normalization.

mod model;
mod prepared;
mod request;
mod response;
mod retention;
mod secret;

pub(crate) use model::{ExpireDelegationTokenRequestRef, NormalizedExpireDelegationTokenResponse};
pub(crate) use prepared::PreparedExpireDelegationTokenRequest;
pub(crate) use request::{ExpireDelegationTokenRequestFailure, expire_delegation_token_request};
pub(crate) use response::{
    ExpireDelegationTokenResponseFailure, normalize_expire_delegation_token_response,
};
#[cfg(test)]
pub(crate) use retention::EXPIRE_DELEGATION_TOKEN_MAX_RETAINED_BYTES;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
