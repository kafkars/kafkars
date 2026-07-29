//! Generated API-key 41 v1-v3 adaptation for bounded token descriptions.

mod correlation;
mod model;
mod prepared;
mod request;
mod response;
mod retention;
mod secret;
mod shape;
mod validation;

#[cfg(test)]
mod fixture;

pub(crate) use model::{
    DescribeDelegationTokenPrincipalRef, DescribeDelegationTokensRequestRef,
    NormalizedDescribeDelegationTokenPrincipal, NormalizedDescribeDelegationTokensResponse,
    NormalizedDescribedDelegationToken,
};
pub(crate) use prepared::PreparedDescribeDelegationTokensRequest;
pub(crate) use request::{
    DescribeDelegationTokensRequestFailure, describe_delegation_tokens_request,
};
pub(crate) use response::normalize_describe_delegation_tokens_response;
#[cfg(test)]
pub(crate) use retention::DESCRIBE_DELEGATION_TOKENS_MAX_RETAINED_BYTES;
pub(crate) use secret::DescribeDelegationTokenHmac;
pub(crate) use validation::DescribeDelegationTokensResponseFailure;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
