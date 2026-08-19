//! Validate-first bounded normalization of generated client-quota facts.

use kafka_wire::DescribeClientQuotasResponse;

use super::{
    NormalizedClientQuotaEntityComponent, NormalizedClientQuotaEntry, NormalizedClientQuotaValue,
    NormalizedDescribeClientQuotasResponse,
    retention::{
        CanonicalEntryRef, bounded_diagnostic_len, normalized_retained_charge, response_peak_charge,
    },
    validation::{canonicalize, validate_response_shape},
};

/// Malformed generated response or insufficient bounded result capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeClientQuotasResponseFailure {
    UnsupportedApiVersion { actual: i16 },
    NegativeThrottleTime { actual: i32 },
    MissingEntriesOnSuccess,
    EntriesWithTopLevelError { actual: usize },
    TooManyEntries { actual: usize, max: usize },
    EmptyEntity,
    TooManyEntityComponents { actual: usize, max: usize },
    EmptyEntityType,
    EntityTypeTooLong { actual: usize, max: usize },
    EmptyEntityName,
    EntityNameTooLong { actual: usize, max: usize },
    EmptyValues,
    TooManyQuotaValues { actual: usize, max: usize },
    EmptyQuotaKey,
    QuotaKeyTooLong { actual: usize, max: usize },
    NonFiniteQuotaValue,
    DuplicateEntityType,
    DuplicateQuotaKey,
    DuplicateEntity,
    RetainedBytes { required: usize, limit: usize },
}

/// Normalizes v0-v1 without leaking generated DTOs or partially built results.
pub(crate) fn normalize_describe_client_quotas_response(
    selected_version: i16,
    response: &DescribeClientQuotasResponse,
    retained_limit: usize,
) -> Result<NormalizedDescribeClientQuotasResponse, DescribeClientQuotasResponseFailure> {
    validate_response_shape(selected_version, response)?;
    let required = response_peak_charge(response).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;
    let canonical = canonicalize(
        response.entries.as_deref().unwrap_or_default(),
        required,
        retained_limit,
    )?;
    materialize(response, canonical, required, retained_limit)
}

fn materialize(
    response: &DescribeClientQuotasResponse,
    canonical: Vec<CanonicalEntryRef<'_>>,
    required: usize,
    limit: usize,
) -> Result<NormalizedDescribeClientQuotasResponse, DescribeClientQuotasResponseFailure> {
    let diagnostic_len = bounded_diagnostic_len(response.error_message.as_deref());
    let error_message = response
        .error_message
        .as_deref()
        .map(|message| copy_string(&message[..diagnostic_len], required, limit))
        .transpose()?;

    let mut entries = Vec::new();
    entries
        .try_reserve_exact(canonical.len())
        .map_err(|_| retained_failure(required, limit))?;
    for entry in canonical {
        entries.push(materialize_entry(entry, required, limit)?);
    }
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        DescribeClientQuotasResponseFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    let mut normalized = NormalizedDescribeClientQuotasResponse {
        throttle_time_ms,
        error_code: response.error_code,
        error_message,
        error_message_truncated: response
            .error_message
            .as_ref()
            .is_some_and(|message| diagnostic_len < message.len()),
        entries,
        retained_bytes: 0,
    };
    let retained = normalized_retained_charge(&normalized).unwrap_or(usize::MAX);
    ensure_limit(retained, limit)?;
    normalized.retained_bytes = required;
    Ok(normalized)
}

fn materialize_entry(
    source: CanonicalEntryRef<'_>,
    required: usize,
    limit: usize,
) -> Result<NormalizedClientQuotaEntry, DescribeClientQuotasResponseFailure> {
    let mut entity = Vec::new();
    entity
        .try_reserve_exact(source.entity.len())
        .map_err(|_| retained_failure(required, limit))?;
    for component in source.entity {
        entity.push(NormalizedClientQuotaEntityComponent {
            entity_type: copy_string(component.entity_type, required, limit)?,
            entity_name: component
                .entity_name
                .map(|name| copy_string(name, required, limit))
                .transpose()?,
        });
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(source.values.len())
        .map_err(|_| retained_failure(required, limit))?;
    for value in source.values {
        values.push(NormalizedClientQuotaValue {
            key: copy_string(value.key, required, limit)?,
            value: value.value,
        });
    }
    Ok(NormalizedClientQuotaEntry { entity, values })
}

fn copy_string(
    source: &str,
    required: usize,
    limit: usize,
) -> Result<String, DescribeClientQuotasResponseFailure> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(source.len())
        .map_err(|_| retained_failure(required, limit))?;
    owned.push_str(source);
    Ok(owned)
}

fn ensure_limit(required: usize, limit: usize) -> Result<(), DescribeClientQuotasResponseFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(DescribeClientQuotasResponseFailure::RetainedBytes { required, limit })
}

const fn retained_failure(required: usize, limit: usize) -> DescribeClientQuotasResponseFailure {
    DescribeClientQuotasResponseFailure::RetainedBytes { required, limit }
}
