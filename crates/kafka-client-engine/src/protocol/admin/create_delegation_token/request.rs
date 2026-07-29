//! Bounded request validation and generated-form materialization.

use kafka_wire::{
    CreateDelegationTokenRequest, create_delegation_token_request::CreatableRenewers,
};
use kafka_wire_core::StrBytes;

use super::{
    CreateDelegationTokenRequestRef, DelegationTokenPrincipalRef,
    prepared::{
        DEFAULT_OWNER_MIN_VERSION, EXPLICIT_OWNER_MIN_VERSION, PreparedCreateDelegationTokenRequest,
    },
    retention::{
        CREATE_DELEGATION_TOKEN_MAX_RETAINED_BYTES, MAX_PRINCIPAL_NAME_BYTES,
        MAX_PRINCIPAL_TYPE_BYTES, MAX_RENEWERS, MAX_REQUEST_TEXT_BYTES, request_charge,
    },
};

/// Invalid intent, allocation failure, or insufficient retained capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreateDelegationTokenRequestFailure {
    InvalidMaxLifetime {
        actual: i64,
    },
    TooManyRenewers {
        actual: usize,
        max: usize,
    },
    EmptyPrincipalType {
        field: &'static str,
    },
    PrincipalTypeTooLong {
        field: &'static str,
        actual: usize,
        max: usize,
    },
    EmptyPrincipalName {
        field: &'static str,
    },
    PrincipalNameTooLong {
        field: &'static str,
        actual: usize,
        max: usize,
    },
    PrincipalTextBytesExceeded {
        required: usize,
        max: usize,
    },
    RetainedBytes {
        required: usize,
        limit: usize,
    },
    Allocation {
        field: &'static str,
        requested: usize,
    },
}

/// Copies one bounded request and returns the exact version floor it needs.
///
/// A default owner retains two generated representations: v1/v2 require the
/// generated absent-field sentinel, while v3 must encode null. Encoding still
/// delegates wholly to generated `kafka-wire` implementations.
pub(crate) fn create_delegation_token_request(
    source: CreateDelegationTokenRequestRef<'_>,
    retained_limit: usize,
) -> Result<PreparedCreateDelegationTokenRequest, CreateDelegationTokenRequestFailure> {
    validate(source)?;
    let minimum_version = if source.owner().is_some() {
        EXPLICIT_OWNER_MIN_VERSION
    } else {
        DEFAULT_OWNER_MIN_VERSION
    };
    let copies = if minimum_version == DEFAULT_OWNER_MIN_VERSION {
        2
    } else {
        1
    };
    let effective_limit = retained_limit.min(CREATE_DELEGATION_TOKEN_MAX_RETAINED_BYTES);
    let required = request_charge(source, copies).unwrap_or(usize::MAX);
    ensure_limit(required, effective_limit)?;

    let legacy = (minimum_version == DEFAULT_OWNER_MIN_VERSION)
        .then(|| materialize(source, true))
        .transpose()?;
    let modern = materialize(source, false)?;
    let prepared = PreparedCreateDelegationTokenRequest::new(legacy, modern, minimum_version);
    ensure_limit(prepared.retained_heap_bytes(), effective_limit)?;
    Ok(prepared)
}

fn materialize(
    source: CreateDelegationTokenRequestRef<'_>,
    legacy: bool,
) -> Result<CreateDelegationTokenRequest, CreateDelegationTokenRequestFailure> {
    let mut renewers = Vec::new();
    renewers
        .try_reserve_exact(source.renewers().len())
        .map_err(|_| CreateDelegationTokenRequestFailure::Allocation {
            field: "renewers",
            requested: source.renewers().len(),
        })?;
    for renewer in source.renewers() {
        let mut generated = CreatableRenewers::default();
        generated.principal_type = copy_text("renewer_principal_type", renewer.principal_type())?;
        generated.principal_name = copy_text("renewer_principal_name", renewer.principal_name())?;
        renewers.push(generated);
    }
    let (owner_principal_type, owner_principal_name) = if legacy {
        (Some(StrBytes::default()), Some(StrBytes::default()))
    } else {
        match source.owner() {
            Some(owner) => (
                Some(copy_text("owner_principal_type", owner.principal_type())?),
                Some(copy_text("owner_principal_name", owner.principal_name())?),
            ),
            None => (None, None),
        }
    };
    let mut request = CreateDelegationTokenRequest::default();
    request.owner_principal_type = owner_principal_type;
    request.owner_principal_name = owner_principal_name;
    request.renewers = renewers;
    request.max_lifetime_ms = source.max_lifetime_ms();
    Ok(request)
}

fn validate(
    source: CreateDelegationTokenRequestRef<'_>,
) -> Result<(), CreateDelegationTokenRequestFailure> {
    if source.max_lifetime_ms() < -1 {
        return Err(CreateDelegationTokenRequestFailure::InvalidMaxLifetime {
            actual: source.max_lifetime_ms(),
        });
    }
    if source.renewers().len() > MAX_RENEWERS {
        return Err(CreateDelegationTokenRequestFailure::TooManyRenewers {
            actual: source.renewers().len(),
            max: MAX_RENEWERS,
        });
    }
    let mut text_bytes = 0usize;
    if let Some(owner) = source.owner() {
        validate_principal("owner", owner, &mut text_bytes)?;
    }
    for renewer in source.renewers() {
        validate_principal("renewer", *renewer, &mut text_bytes)?;
    }
    if text_bytes > MAX_REQUEST_TEXT_BYTES {
        return Err(
            CreateDelegationTokenRequestFailure::PrincipalTextBytesExceeded {
                required: text_bytes,
                max: MAX_REQUEST_TEXT_BYTES,
            },
        );
    }
    Ok(())
}

fn validate_principal(
    field: &'static str,
    principal: DelegationTokenPrincipalRef<'_>,
    text_bytes: &mut usize,
) -> Result<(), CreateDelegationTokenRequestFailure> {
    if principal.principal_type().is_empty() {
        return Err(CreateDelegationTokenRequestFailure::EmptyPrincipalType { field });
    }
    if principal.principal_type().len() > MAX_PRINCIPAL_TYPE_BYTES {
        return Err(CreateDelegationTokenRequestFailure::PrincipalTypeTooLong {
            field,
            actual: principal.principal_type().len(),
            max: MAX_PRINCIPAL_TYPE_BYTES,
        });
    }
    if principal.principal_name().is_empty() {
        return Err(CreateDelegationTokenRequestFailure::EmptyPrincipalName { field });
    }
    if principal.principal_name().len() > MAX_PRINCIPAL_NAME_BYTES {
        return Err(CreateDelegationTokenRequestFailure::PrincipalNameTooLong {
            field,
            actual: principal.principal_name().len(),
            max: MAX_PRINCIPAL_NAME_BYTES,
        });
    }
    *text_bytes = text_bytes
        .checked_add(principal.principal_type().len())
        .and_then(|bytes| bytes.checked_add(principal.principal_name().len()))
        .unwrap_or(usize::MAX);
    Ok(())
}

fn copy_text(
    field: &'static str,
    source: &str,
) -> Result<StrBytes, CreateDelegationTokenRequestFailure> {
    let mut owned = String::new();
    owned.try_reserve_exact(source.len()).map_err(|_| {
        CreateDelegationTokenRequestFailure::Allocation {
            field,
            requested: source.len(),
        }
    })?;
    owned.push_str(source);
    Ok(owned.into())
}

fn ensure_limit(required: usize, limit: usize) -> Result<(), CreateDelegationTokenRequestFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(CreateDelegationTokenRequestFailure::RetainedBytes { required, limit })
}
