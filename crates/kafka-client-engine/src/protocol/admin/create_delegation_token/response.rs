//! Validate-first normalization of one secret-bearing API-key 38 terminal.

use kafka_wire::CreateDelegationTokenResponse;

use super::{
    DelegationTokenHmac, NormalizedCreateDelegationTokenResponse, NormalizedDelegationToken,
    NormalizedDelegationTokenPrincipal,
    retention::{
        CREATE_DELEGATION_TOKEN_MAX_RETAINED_BYTES, error_charge, normalized_charge,
        success_source_charge,
    },
    validation::{
        CreateDelegationTokenResponseFailure, MAX_VERSION, MIN_VERSION, validate_success,
    },
};

/// Preserves exact signed broker status and copies success secrets once.
pub(crate) fn normalize_create_delegation_token_response(
    selected_version: Option<i16>,
    response: &CreateDelegationTokenResponse,
    retained_limit: usize,
) -> Result<NormalizedCreateDelegationTokenResponse, CreateDelegationTokenResponseFailure> {
    let selected_version =
        selected_version.ok_or(CreateDelegationTokenResponseFailure::MissingSelectedVersion)?;
    if !(MIN_VERSION..=MAX_VERSION).contains(&selected_version) {
        return Err(
            CreateDelegationTokenResponseFailure::UnsupportedApiVersion {
                actual: selected_version,
            },
        );
    }
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        CreateDelegationTokenResponseFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    let effective_limit = retained_limit.min(CREATE_DELEGATION_TOKEN_MAX_RETAINED_BYTES);
    if response.error_code != 0 {
        let required = error_charge();
        ensure_limit(required, effective_limit)?;
        return Ok(NormalizedCreateDelegationTokenResponse::new(
            throttle_time_ms,
            response.error_code,
            None,
            required,
        ));
    }

    validate_success(selected_version, response)?;
    let include_requester = selected_version >= 3;
    let source_required = success_source_charge(response, include_requester).unwrap_or(usize::MAX);
    ensure_limit(source_required, effective_limit)?;
    let owner = copy_principal(
        "owner",
        response.principal_type.as_str(),
        response.principal_name.as_str(),
    )?;
    let requester = include_requester
        .then(|| {
            copy_principal(
                "requester",
                response.token_requester_principal_type.as_str(),
                response.token_requester_principal_name.as_str(),
            )
        })
        .transpose()?;
    let token_id = copy_string("token_id", response.token_id.as_str())?;
    let hmac = copy_bytes("hmac", response.hmac.as_ref())?;
    let token = NormalizedDelegationToken::new(
        owner,
        requester,
        response.issue_timestamp_ms,
        response.expiry_timestamp_ms,
        response.max_timestamp_ms,
        token_id,
        DelegationTokenHmac::new(hmac),
    );
    let mut normalized = NormalizedCreateDelegationTokenResponse::new(
        throttle_time_ms,
        response.error_code,
        Some(token),
        0,
    );
    let retained = normalized_charge(&normalized).unwrap_or(usize::MAX);
    ensure_limit(retained, effective_limit)?;
    normalized = NormalizedCreateDelegationTokenResponse::new(
        throttle_time_ms,
        response.error_code,
        normalized.into_parts().2,
        source_required.max(retained),
    );
    Ok(normalized)
}

fn copy_principal(
    field: &'static str,
    principal_type: &str,
    principal_name: &str,
) -> Result<NormalizedDelegationTokenPrincipal, CreateDelegationTokenResponseFailure> {
    Ok(NormalizedDelegationTokenPrincipal::new(
        copy_string(field, principal_type)?,
        copy_string(field, principal_name)?,
    ))
}

fn copy_string(
    field: &'static str,
    source: &str,
) -> Result<String, CreateDelegationTokenResponseFailure> {
    let mut owned = String::new();
    owned.try_reserve_exact(source.len()).map_err(|_| {
        CreateDelegationTokenResponseFailure::Allocation {
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
) -> Result<Vec<u8>, CreateDelegationTokenResponseFailure> {
    let mut owned = Vec::new();
    owned.try_reserve_exact(source.len()).map_err(|_| {
        CreateDelegationTokenResponseFailure::Allocation {
            field,
            requested: source.len(),
        }
    })?;
    owned.extend_from_slice(source);
    Ok(owned)
}

fn ensure_limit(required: usize, limit: usize) -> Result<(), CreateDelegationTokenResponseFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(CreateDelegationTokenResponseFailure::RetainedBytes { required, limit })
}
