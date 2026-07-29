//! Allocation-free selection, token, principal, and secret-shape checks.

use kafka_wire::describe_delegation_token_response::DescribedDelegationToken;

use super::{
    DescribeDelegationTokenPrincipalRef, DescribeDelegationTokensRequestRef,
    DescribeDelegationTokensResponseFailure,
    retention::{
        MAX_HMAC_BYTES, MAX_OWNERS, MAX_PRINCIPAL_NAME_BYTES, MAX_PRINCIPAL_TYPE_BYTES,
        MAX_RENEWERS_PER_TOKEN, MAX_TOKEN_ID_BYTES,
    },
};

pub(super) fn validate_selection(
    request: DescribeDelegationTokensRequestRef<'_>,
) -> Result<(), DescribeDelegationTokensResponseFailure> {
    let Some(owners) = request.owners() else {
        return Ok(());
    };
    if owners.is_empty() {
        return Err(DescribeDelegationTokensResponseFailure::EmptyOwnerSelection);
    }
    if owners.len() > MAX_OWNERS {
        return Err(
            DescribeDelegationTokensResponseFailure::TooManyRequestedOwners {
                actual: owners.len(),
                max: MAX_OWNERS,
            },
        );
    }
    for (index, owner) in owners.iter().enumerate() {
        validate_principal_ref("requested_owner", *owner)?;
        if owners[..index].contains(owner) {
            return Err(DescribeDelegationTokensResponseFailure::DuplicateRequestedOwner);
        }
    }
    Ok(())
}

pub(super) fn validate_token(
    selected_version: i16,
    request: DescribeDelegationTokensRequestRef<'_>,
    token: &DescribedDelegationToken,
) -> Result<(), DescribeDelegationTokensResponseFailure> {
    validate_principal(
        "owner",
        token.principal_type.as_str(),
        token.principal_name.as_str(),
    )?;
    if !owner_is_selected(request, token) {
        return Err(DescribeDelegationTokensResponseFailure::UnexpectedOwner);
    }
    if selected_version < 3 {
        if !token.token_requester_principal_type.is_empty()
            || !token.token_requester_principal_name.is_empty()
        {
            return Err(DescribeDelegationTokensResponseFailure::UnexpectedRequesterBeforeV3);
        }
    } else {
        validate_principal(
            "requester",
            token.token_requester_principal_type.as_str(),
            token.token_requester_principal_name.as_str(),
        )?;
    }
    validate_token_scalar(token)?;
    validate_renewers(token)
}

fn validate_token_scalar(
    token: &DescribedDelegationToken,
) -> Result<(), DescribeDelegationTokensResponseFailure> {
    if token.issue_timestamp < 0 {
        return Err(
            DescribeDelegationTokensResponseFailure::InvalidIssueTimestamp {
                actual: token.issue_timestamp,
            },
        );
    }
    if token.expiry_timestamp < token.issue_timestamp {
        return Err(
            DescribeDelegationTokensResponseFailure::InvalidExpiryTimestamp {
                issue: token.issue_timestamp,
                expiry: token.expiry_timestamp,
            },
        );
    }
    if token.max_timestamp < token.expiry_timestamp {
        return Err(
            DescribeDelegationTokensResponseFailure::InvalidMaxTimestamp {
                expiry: token.expiry_timestamp,
                max: token.max_timestamp,
            },
        );
    }
    if token.token_id.is_empty() {
        return Err(DescribeDelegationTokensResponseFailure::EmptyTokenId);
    }
    if token.token_id.len() > MAX_TOKEN_ID_BYTES {
        return Err(DescribeDelegationTokensResponseFailure::TokenIdTooLong {
            actual: token.token_id.len(),
            max: MAX_TOKEN_ID_BYTES,
        });
    }
    if token.hmac.is_empty() {
        return Err(DescribeDelegationTokensResponseFailure::EmptyHmac);
    }
    if token.hmac.len() > MAX_HMAC_BYTES {
        return Err(DescribeDelegationTokensResponseFailure::HmacTooLong {
            actual: token.hmac.len(),
            max: MAX_HMAC_BYTES,
        });
    }
    Ok(())
}

fn validate_renewers(
    token: &DescribedDelegationToken,
) -> Result<(), DescribeDelegationTokensResponseFailure> {
    if token.renewers.len() > MAX_RENEWERS_PER_TOKEN {
        return Err(DescribeDelegationTokensResponseFailure::TooManyRenewers {
            actual: token.renewers.len(),
            max: MAX_RENEWERS_PER_TOKEN,
        });
    }
    for (index, renewer) in token.renewers.iter().enumerate() {
        validate_principal(
            "renewer",
            renewer.principal_type.as_str(),
            renewer.principal_name.as_str(),
        )?;
        if token.renewers[..index].iter().any(|prior| {
            prior.principal_type == renewer.principal_type
                && prior.principal_name == renewer.principal_name
        }) {
            return Err(DescribeDelegationTokensResponseFailure::DuplicateRenewer);
        }
    }
    Ok(())
}

fn owner_is_selected(
    request: DescribeDelegationTokensRequestRef<'_>,
    token: &DescribedDelegationToken,
) -> bool {
    request.owners().is_none_or(|owners| {
        owners.iter().any(|owner| {
            owner.principal_type() == token.principal_type.as_str()
                && owner.principal_name() == token.principal_name.as_str()
        })
    })
}

fn validate_principal_ref(
    field: &'static str,
    principal: DescribeDelegationTokenPrincipalRef<'_>,
) -> Result<(), DescribeDelegationTokensResponseFailure> {
    validate_principal(
        field,
        principal.principal_type(),
        principal.principal_name(),
    )
}

fn validate_principal(
    field: &'static str,
    principal_type: &str,
    principal_name: &str,
) -> Result<(), DescribeDelegationTokensResponseFailure> {
    if principal_type.is_empty() {
        return Err(DescribeDelegationTokensResponseFailure::EmptyPrincipalType { field });
    }
    if principal_type.len() > MAX_PRINCIPAL_TYPE_BYTES {
        return Err(
            DescribeDelegationTokensResponseFailure::PrincipalTypeTooLong {
                field,
                actual: principal_type.len(),
                max: MAX_PRINCIPAL_TYPE_BYTES,
            },
        );
    }
    if principal_name.is_empty() {
        return Err(DescribeDelegationTokensResponseFailure::EmptyPrincipalName { field });
    }
    if principal_name.len() > MAX_PRINCIPAL_NAME_BYTES {
        return Err(
            DescribeDelegationTokensResponseFailure::PrincipalNameTooLong {
                field,
                actual: principal_name.len(),
                max: MAX_PRINCIPAL_NAME_BYTES,
            },
        );
    }
    Ok(())
}
