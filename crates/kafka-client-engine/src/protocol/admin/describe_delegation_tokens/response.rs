//! Validate-first normalization of a complete secret-bearing token list.

use kafka_wire::{
    DescribeDelegationTokenResponse,
    describe_delegation_token_response::{
        DescribedDelegationToken, DescribedDelegationTokenRenewer,
    },
};

use super::{
    DescribeDelegationTokenHmac, DescribeDelegationTokensRequestRef,
    DescribeDelegationTokensResponseFailure, NormalizedDescribeDelegationTokenPrincipal,
    NormalizedDescribeDelegationTokensResponse, NormalizedDescribedDelegationToken,
    correlation::{ordered_renewers, ordered_tokens},
    retention::{
        DESCRIBE_DELEGATION_TOKENS_MAX_RETAINED_BYTES, error_charge, normalized_charge,
        response_peak_charge,
    },
    validation::validate_response,
};

/// Preserves exact broker status and either owns every token or no token.
pub(crate) fn normalize_describe_delegation_tokens_response(
    selected_version: Option<i16>,
    request: DescribeDelegationTokensRequestRef<'_>,
    response: &DescribeDelegationTokenResponse,
    retained_limit: usize,
) -> Result<NormalizedDescribeDelegationTokensResponse, DescribeDelegationTokensResponseFailure> {
    let selected_version =
        selected_version.ok_or(DescribeDelegationTokensResponseFailure::MissingSelectedVersion)?;
    validate_response(selected_version, request, response)?;
    let effective_limit = retained_limit.min(DESCRIBE_DELEGATION_TOKENS_MAX_RETAINED_BYTES);
    let throttle = response.throttle_time_ms as u32;
    if response.error_code != 0 {
        let required = error_charge();
        ensure_limit(required, effective_limit)?;
        return Ok(NormalizedDescribeDelegationTokensResponse::new(
            throttle,
            response.error_code,
            Vec::new(),
            required,
        ));
    }

    let include_requester = selected_version >= 3;
    let required = response_peak_charge(response, include_requester).unwrap_or(usize::MAX);
    ensure_limit(required, effective_limit)?;
    let ordered = ordered_tokens(&response.tokens, required, effective_limit)?;
    let mut tokens = Vec::new();
    tokens.try_reserve_exact(ordered.len()).map_err(|_| {
        DescribeDelegationTokensResponseFailure::Allocation {
            field: "tokens",
            requested: ordered.len(),
        }
    })?;
    for token in ordered {
        tokens.push(materialize_token(
            token,
            include_requester,
            required,
            effective_limit,
        )?);
    }
    let mut normalized =
        NormalizedDescribeDelegationTokensResponse::new(throttle, response.error_code, tokens, 0);
    let retained = normalized_charge(&normalized).unwrap_or(usize::MAX);
    ensure_limit(retained, effective_limit)?;
    normalized = NormalizedDescribeDelegationTokensResponse::new(
        throttle,
        response.error_code,
        normalized.into_parts().2,
        required.max(retained),
    );
    Ok(normalized)
}

fn materialize_token(
    source: &DescribedDelegationToken,
    include_requester: bool,
    required: usize,
    limit: usize,
) -> Result<NormalizedDescribedDelegationToken, DescribeDelegationTokensResponseFailure> {
    let owner = copy_principal(
        "owner",
        source.principal_type.as_str(),
        source.principal_name.as_str(),
    )?;
    let requester = include_requester
        .then(|| {
            copy_principal(
                "requester",
                source.token_requester_principal_type.as_str(),
                source.token_requester_principal_name.as_str(),
            )
        })
        .transpose()?;
    let renewers = materialize_renewers(&source.renewers, required, limit)?;
    Ok(NormalizedDescribedDelegationToken::new(
        owner,
        requester,
        source.issue_timestamp,
        source.expiry_timestamp,
        source.max_timestamp,
        copy_string("token_id", source.token_id.as_str())?,
        DescribeDelegationTokenHmac::new(copy_bytes("hmac", source.hmac.as_ref())?),
        renewers,
    ))
}

fn materialize_renewers(
    source: &[DescribedDelegationTokenRenewer],
    required: usize,
    limit: usize,
) -> Result<Vec<NormalizedDescribeDelegationTokenPrincipal>, DescribeDelegationTokensResponseFailure>
{
    let ordered = ordered_renewers(source, required, limit)?;
    let mut renewers = Vec::new();
    renewers.try_reserve_exact(ordered.len()).map_err(|_| {
        DescribeDelegationTokensResponseFailure::Allocation {
            field: "renewers",
            requested: ordered.len(),
        }
    })?;
    for renewer in ordered {
        renewers.push(copy_principal(
            "renewer",
            renewer.principal_type.as_str(),
            renewer.principal_name.as_str(),
        )?);
    }
    Ok(renewers)
}

fn copy_principal(
    field: &'static str,
    principal_type: &str,
    principal_name: &str,
) -> Result<NormalizedDescribeDelegationTokenPrincipal, DescribeDelegationTokensResponseFailure> {
    Ok(NormalizedDescribeDelegationTokenPrincipal::new(
        copy_string(field, principal_type)?,
        copy_string(field, principal_name)?,
    ))
}

fn copy_string(
    field: &'static str,
    source: &str,
) -> Result<String, DescribeDelegationTokensResponseFailure> {
    let mut owned = String::new();
    owned.try_reserve_exact(source.len()).map_err(|_| {
        DescribeDelegationTokensResponseFailure::Allocation {
            field,
            requested: source.len(),
        }
    })?;
    owned.push_str(source);
    Ok(owned)
}

fn copy_bytes(
    field: &'static str,
    source: &[u8],
) -> Result<Vec<u8>, DescribeDelegationTokensResponseFailure> {
    let mut owned = Vec::new();
    owned.try_reserve_exact(source.len()).map_err(|_| {
        DescribeDelegationTokensResponseFailure::Allocation {
            field,
            requested: source.len(),
        }
    })?;
    owned.extend_from_slice(source);
    Ok(owned)
}

fn ensure_limit(
    required: usize,
    limit: usize,
) -> Result<(), DescribeDelegationTokensResponseFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(DescribeDelegationTokensResponseFailure::RetainedBytes { required, limit })
}
