//! Checked request and secret-bearing terminal retention bounds.

use core::mem::size_of;

use kafka_wire::{
    CreateDelegationTokenRequest, create_delegation_token_request::CreatableRenewers,
};

use super::{
    CreateDelegationTokenRequestRef, NormalizedCreateDelegationTokenResponse,
    NormalizedDelegationToken, NormalizedDelegationTokenPrincipal,
};

pub(crate) const CREATE_DELEGATION_TOKEN_MAX_RETAINED_BYTES: usize = 64 * 1_024;
pub(super) const MAX_RENEWERS: usize = 1_024;
pub(super) const MAX_PRINCIPAL_TYPE_BYTES: usize = 128;
pub(super) const MAX_PRINCIPAL_NAME_BYTES: usize = 4 * 1_024;
pub(super) const MAX_REQUEST_TEXT_BYTES: usize = 32 * 1_024;
pub(super) const MAX_TOKEN_ID_BYTES: usize = 4 * 1_024;
pub(super) const MAX_HMAC_BYTES: usize = 4 * 1_024;

pub(super) fn request_charge(
    source: CreateDelegationTokenRequestRef<'_>,
    copies: usize,
) -> Option<usize> {
    let owner_text = match source.owner() {
        Some(owner) => owner
            .principal_type()
            .len()
            .checked_add(owner.principal_name().len())?,
        None => 0,
    };
    let renewer_text = source
        .renewers()
        .iter()
        .try_fold(0usize, |bytes, renewer| {
            bytes
                .checked_add(renewer.principal_type().len())?
                .checked_add(renewer.principal_name().len())
        })?;
    let renewer_owners = source
        .renewers()
        .len()
        .checked_mul(size_of::<CreatableRenewers>())?;
    size_of::<super::PreparedCreateDelegationTokenRequest>()
        .checked_add(copies.checked_mul(size_of::<CreateDelegationTokenRequest>())?)?
        .checked_add(copies.checked_mul(renewer_owners)?)?
        .checked_add(copies.checked_mul(owner_text.checked_add(renewer_text)?)?)
}

pub(super) const fn error_charge() -> usize {
    size_of::<NormalizedCreateDelegationTokenResponse>()
}

pub(super) fn success_source_charge(
    response: &kafka_wire::CreateDelegationTokenResponse,
    include_requester: bool,
) -> Option<usize> {
    let requester = include_requester.then_some(
        response
            .token_requester_principal_type
            .len()
            .checked_add(response.token_requester_principal_name.len())?,
    );
    size_of::<NormalizedCreateDelegationTokenResponse>()
        .checked_add(size_of::<NormalizedDelegationToken>())?
        .checked_add(size_of::<NormalizedDelegationTokenPrincipal>())?
        .checked_add(
            include_requester
                .then_some(size_of::<NormalizedDelegationTokenPrincipal>())
                .unwrap_or(0),
        )?
        .checked_add(response.principal_type.len())?
        .checked_add(response.principal_name.len())?
        .checked_add(requester.unwrap_or(0))?
        .checked_add(response.token_id.len())?
        .checked_add(response.hmac.len())
}

pub(super) fn normalized_charge(
    response: &NormalizedCreateDelegationTokenResponse,
) -> Option<usize> {
    size_of::<NormalizedCreateDelegationTokenResponse>().checked_add(
        response
            .token()
            .map_or(Some(0), NormalizedDelegationToken::retained_capacity)?,
    )
}
