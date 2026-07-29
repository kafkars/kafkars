//! Validate-first construction of a generated nullable-owner request.

use kafka_wire::{
    DescribeDelegationTokenRequest, describe_delegation_token_request::DescribeDelegationTokenOwner,
};
use kafka_wire_core::StrBytes;

use super::{
    DescribeDelegationTokenPrincipalRef, DescribeDelegationTokensRequestRef,
    PreparedDescribeDelegationTokensRequest,
    retention::{
        DESCRIBE_DELEGATION_TOKENS_MAX_RETAINED_BYTES, MAX_OWNERS, MAX_PRINCIPAL_NAME_BYTES,
        MAX_PRINCIPAL_TYPE_BYTES, request_charge,
    },
};

/// Invalid selection, failed allocation, or exhausted retained capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeDelegationTokensRequestFailure {
    EmptyOwnerSelection,
    TooManyOwners {
        actual: usize,
        max: usize,
    },
    EmptyPrincipalType,
    PrincipalTypeTooLong {
        actual: usize,
        max: usize,
    },
    EmptyPrincipalName,
    PrincipalNameTooLong {
        actual: usize,
        max: usize,
    },
    DuplicateOwner,
    RetainedBytes {
        required: usize,
        limit: usize,
    },
    Allocation {
        field: &'static str,
        requested: usize,
    },
}

/// Copies one bounded explicit selection into generated API-key 41 ownership.
pub(crate) fn describe_delegation_tokens_request(
    source: DescribeDelegationTokensRequestRef<'_>,
    retained_limit: usize,
) -> Result<PreparedDescribeDelegationTokensRequest, DescribeDelegationTokensRequestFailure> {
    validate(source)?;
    let effective_limit = retained_limit.min(DESCRIBE_DELEGATION_TOKENS_MAX_RETAINED_BYTES);
    let required = request_charge(source).unwrap_or(usize::MAX);
    ensure_limit(required, effective_limit)?;

    let owners = source
        .owners()
        .map(|owners| materialize(owners))
        .transpose()?;
    let mut request = DescribeDelegationTokenRequest::default();
    request.owners = owners;
    let prepared = PreparedDescribeDelegationTokensRequest::new(request);
    ensure_limit(prepared.retained_heap_bytes(), effective_limit)?;
    Ok(prepared)
}

fn validate(
    source: DescribeDelegationTokensRequestRef<'_>,
) -> Result<(), DescribeDelegationTokensRequestFailure> {
    let Some(owners) = source.owners() else {
        return Ok(());
    };
    if owners.is_empty() {
        return Err(DescribeDelegationTokensRequestFailure::EmptyOwnerSelection);
    }
    if owners.len() > MAX_OWNERS {
        return Err(DescribeDelegationTokensRequestFailure::TooManyOwners {
            actual: owners.len(),
            max: MAX_OWNERS,
        });
    }
    for (index, owner) in owners.iter().enumerate() {
        validate_principal(*owner)?;
        if owners[..index].contains(owner) {
            return Err(DescribeDelegationTokensRequestFailure::DuplicateOwner);
        }
    }
    Ok(())
}

fn validate_principal(
    principal: DescribeDelegationTokenPrincipalRef<'_>,
) -> Result<(), DescribeDelegationTokensRequestFailure> {
    if principal.principal_type().is_empty() {
        return Err(DescribeDelegationTokensRequestFailure::EmptyPrincipalType);
    }
    if principal.principal_type().len() > MAX_PRINCIPAL_TYPE_BYTES {
        return Err(
            DescribeDelegationTokensRequestFailure::PrincipalTypeTooLong {
                actual: principal.principal_type().len(),
                max: MAX_PRINCIPAL_TYPE_BYTES,
            },
        );
    }
    if principal.principal_name().is_empty() {
        return Err(DescribeDelegationTokensRequestFailure::EmptyPrincipalName);
    }
    if principal.principal_name().len() > MAX_PRINCIPAL_NAME_BYTES {
        return Err(
            DescribeDelegationTokensRequestFailure::PrincipalNameTooLong {
                actual: principal.principal_name().len(),
                max: MAX_PRINCIPAL_NAME_BYTES,
            },
        );
    }
    Ok(())
}

fn materialize(
    owners: &[DescribeDelegationTokenPrincipalRef<'_>],
) -> Result<Vec<DescribeDelegationTokenOwner>, DescribeDelegationTokensRequestFailure> {
    let mut generated = Vec::new();
    generated.try_reserve_exact(owners.len()).map_err(|_| {
        DescribeDelegationTokensRequestFailure::Allocation {
            field: "owners",
            requested: owners.len(),
        }
    })?;
    for owner in owners {
        let mut generated_owner = DescribeDelegationTokenOwner::default();
        generated_owner.principal_type = copy_text("owner_principal_type", owner.principal_type())?;
        generated_owner.principal_name = copy_text("owner_principal_name", owner.principal_name())?;
        generated.push(generated_owner);
    }
    Ok(generated)
}

fn copy_text(
    field: &'static str,
    source: &str,
) -> Result<StrBytes, DescribeDelegationTokensRequestFailure> {
    let mut owned = String::new();
    owned.try_reserve_exact(source.len()).map_err(|_| {
        DescribeDelegationTokensRequestFailure::Allocation {
            field,
            requested: source.len(),
        }
    })?;
    owned.push_str(source);
    Ok(owned.into())
}

fn ensure_limit(
    required: usize,
    limit: usize,
) -> Result<(), DescribeDelegationTokensRequestFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(DescribeDelegationTokensRequestFailure::RetainedBytes { required, limit })
}
