//! Checked request, response, scratch, and secret-retention bounds.

use core::mem::size_of;

use kafka_wire::{
    DescribeDelegationTokenRequest,
    describe_delegation_token_request::DescribeDelegationTokenOwner,
    describe_delegation_token_response::{
        DescribedDelegationToken, DescribedDelegationTokenRenewer,
    },
};

use super::{
    DescribeDelegationTokensRequestRef, NormalizedDescribeDelegationTokenPrincipal,
    NormalizedDescribeDelegationTokensResponse, NormalizedDescribedDelegationToken,
};

pub(crate) const DESCRIBE_DELEGATION_TOKENS_MAX_RETAINED_BYTES: usize = 4 * 1_024 * 1_024;
pub(super) const MAX_OWNERS: usize = 4 * 1_024;
pub(super) const MAX_TOKENS: usize = 4 * 1_024;
pub(super) const MAX_RENEWERS_PER_TOKEN: usize = 4 * 1_024;
pub(super) const MAX_PRINCIPAL_TYPE_BYTES: usize = 128;
pub(super) const MAX_PRINCIPAL_NAME_BYTES: usize = 4 * 1_024;
pub(super) const MAX_TOKEN_ID_BYTES: usize = 4 * 1_024;
pub(super) const MAX_HMAC_BYTES: usize = 4 * 1_024;

pub(super) fn request_charge(source: DescribeDelegationTokensRequestRef<'_>) -> Option<usize> {
    let owners = source.owners().unwrap_or_default();
    let text = owners.iter().try_fold(0usize, |bytes, owner| {
        bytes
            .checked_add(owner.principal_type().len())?
            .checked_add(owner.principal_name().len())
    })?;
    size_of::<super::PreparedDescribeDelegationTokensRequest>()
        .checked_add(size_of::<DescribeDelegationTokenRequest>())?
        .checked_add(
            owners
                .len()
                .checked_mul(size_of::<DescribeDelegationTokenOwner>())?,
        )?
        .checked_add(text)
}

pub(super) const fn error_charge() -> usize {
    size_of::<NormalizedDescribeDelegationTokensResponse>()
}

pub(super) fn response_peak_charge(
    response: &kafka_wire::DescribeDelegationTokenResponse,
    include_requester: bool,
) -> Option<usize> {
    let mut bytes = size_of::<NormalizedDescribeDelegationTokensResponse>()
        .checked_add(
            response
                .tokens
                .len()
                .checked_mul(size_of::<NormalizedDescribedDelegationToken>())?,
        )?
        .checked_add(
            response
                .tokens
                .len()
                .checked_mul(size_of::<&DescribedDelegationToken>())?,
        )?;
    for token in &response.tokens {
        bytes = bytes
            .checked_add(size_of::<NormalizedDescribeDelegationTokenPrincipal>())?
            .checked_add(
                include_requester
                    .then_some(size_of::<NormalizedDescribeDelegationTokenPrincipal>())
                    .unwrap_or(0),
            )?
            .checked_add(token.principal_type.len())?
            .checked_add(token.principal_name.len())?
            .checked_add(token.token_id.len())?
            .checked_add(token.hmac.len())?
            .checked_add(
                include_requester
                    .then_some(
                        token
                            .token_requester_principal_type
                            .len()
                            .checked_add(token.token_requester_principal_name.len())?,
                    )
                    .unwrap_or(0),
            )?
            .checked_add(
                token
                    .renewers
                    .len()
                    .checked_mul(size_of::<NormalizedDescribeDelegationTokenPrincipal>())?,
            )?
            .checked_add(
                token
                    .renewers
                    .len()
                    .checked_mul(size_of::<&DescribedDelegationTokenRenewer>())?,
            )?;
        for renewer in &token.renewers {
            bytes = bytes
                .checked_add(renewer.principal_type.len())?
                .checked_add(renewer.principal_name.len())?;
        }
    }
    Some(bytes)
}

pub(super) fn normalized_charge(
    response: &NormalizedDescribeDelegationTokensResponse,
) -> Option<usize> {
    size_of::<NormalizedDescribeDelegationTokensResponse>()
        .checked_add(
            response
                .token_capacity()
                .checked_mul(size_of::<NormalizedDescribedDelegationToken>())?,
        )?
        .checked_add(response.tokens().iter().try_fold(0usize, |bytes, token| {
            bytes.checked_add(token.retained_capacity()?)
        })?)
}
