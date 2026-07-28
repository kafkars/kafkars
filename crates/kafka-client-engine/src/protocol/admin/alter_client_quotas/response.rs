//! Validate-first correlation and bounded materialization of entity outcomes.

use kafka_wire::{
    AlterClientQuotasResponse, alter_client_quotas_response::EntryData as ResponseEntryData,
};

use super::{
    AlterClientQuotasRequestFailure, AlterClientQuotasRequestRef,
    NormalizedAlterClientQuotaEntityComponent, NormalizedAlterClientQuotaOutcome,
    NormalizedAlterClientQuotasResponse,
    request_validation::{CanonicalAlterationRef, canonicalize_request},
    response_retention::{
        bounded_diagnostic_len, normalized_retained_charge, response_peak_charge,
    },
    response_validation::{CanonicalResponseEntryRef, canonicalize_response_entries},
    retention::MAX_ALTERATIONS,
    version::supports_alter_client_quotas_version,
};

/// Malformed generated response or insufficient bounded result capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterClientQuotasResponseFailure {
    UnsupportedApiVersion { actual: i16 },
    NegativeThrottleTime { actual: i32 },
    EntryCount { expected: usize, actual: usize },
    TooManyEntries { actual: usize, max: usize },
    EmptyEntity,
    TooManyEntityComponents { actual: usize, max: usize },
    EmptyEntityType,
    EntityTypeTooLong { actual: usize, max: usize },
    EmptyEntityName,
    EntityNameTooLong { actual: usize, max: usize },
    DuplicateEntityType,
    DuplicateResponseEntity,
    UnexpectedEntity,
    MissingEntity,
    InvalidRequest,
    RetainedBytes { required: usize, limit: usize },
}

/// Normalizes v0-v1, proves one-to-one identity correlation, and restores order.
pub(crate) fn normalize_alter_client_quotas_response(
    selected_version: i16,
    request: AlterClientQuotasRequestRef<'_>,
    response: &AlterClientQuotasResponse,
    retained_limit: usize,
) -> Result<NormalizedAlterClientQuotasResponse, AlterClientQuotasResponseFailure> {
    validate_top_level(selected_version, request, response)?;
    let required = response_peak_charge(request, response).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;
    let expected = canonicalize_request(request, required, retained_limit)
        .map_err(|failure| map_request_failure(failure, required, retained_limit))?;
    let actual = canonicalize_response_entries(&response.entries, required, retained_limit)?;
    let ordered = correlate(&expected, actual, required, retained_limit)?;
    materialize(response, ordered, required, retained_limit)
}

fn validate_top_level(
    selected_version: i16,
    request: AlterClientQuotasRequestRef<'_>,
    response: &AlterClientQuotasResponse,
) -> Result<(), AlterClientQuotasResponseFailure> {
    if !supports_alter_client_quotas_version(selected_version) {
        return Err(AlterClientQuotasResponseFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    if response.throttle_time_ms < 0 {
        return Err(AlterClientQuotasResponseFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        });
    }
    if response.entries.len() > MAX_ALTERATIONS {
        return Err(AlterClientQuotasResponseFailure::TooManyEntries {
            actual: response.entries.len(),
            max: MAX_ALTERATIONS,
        });
    }
    if response.entries.len() != request.alterations().len() {
        return Err(AlterClientQuotasResponseFailure::EntryCount {
            expected: request.alterations().len(),
            actual: response.entries.len(),
        });
    }
    Ok(())
}

fn correlate<'a>(
    expected: &[CanonicalAlterationRef<'_>],
    mut actual: Vec<CanonicalResponseEntryRef<'a>>,
    required: usize,
    limit: usize,
) -> Result<Vec<&'a ResponseEntryData>, AlterClientQuotasResponseFailure> {
    actual.sort_unstable_by(|left, right| left.entity.cmp(&right.entity));
    if actual
        .windows(2)
        .any(|pair| pair[0].entity == pair[1].entity)
    {
        return Err(AlterClientQuotasResponseFailure::DuplicateResponseEntity);
    }

    let mut expected_order = Vec::new();
    expected_order
        .try_reserve_exact(expected.len())
        .map_err(|_| retained_failure(required, limit))?;
    expected_order.extend(0..expected.len());
    expected_order
        .sort_unstable_by(|left, right| expected[*left].entity.cmp(&expected[*right].entity));

    for (expected_index, returned) in expected_order.iter().zip(&actual) {
        match returned.entity.cmp(&expected[*expected_index].entity) {
            core::cmp::Ordering::Less => {
                return Err(AlterClientQuotasResponseFailure::UnexpectedEntity);
            }
            core::cmp::Ordering::Greater => {
                return Err(AlterClientQuotasResponseFailure::MissingEntity);
            }
            core::cmp::Ordering::Equal => {}
        }
    }

    let mut ordered = Vec::new();
    ordered
        .try_reserve_exact(expected.len())
        .map_err(|_| retained_failure(required, limit))?;
    ordered.resize(expected.len(), actual[0].source);
    for (expected_index, returned) in expected_order.into_iter().zip(actual) {
        ordered[expected_index] = returned.source;
    }
    Ok(ordered)
}

fn materialize(
    response: &AlterClientQuotasResponse,
    ordered: Vec<&ResponseEntryData>,
    required: usize,
    limit: usize,
) -> Result<NormalizedAlterClientQuotasResponse, AlterClientQuotasResponseFailure> {
    let mut outcomes = Vec::new();
    outcomes
        .try_reserve_exact(ordered.len())
        .map_err(|_| retained_failure(required, limit))?;
    for entry in ordered {
        outcomes.push(materialize_outcome(entry, required, limit)?);
    }
    let mut normalized = NormalizedAlterClientQuotasResponse {
        throttle_time_ms: response.throttle_time_ms as u32,
        outcomes,
        retained_bytes: 0,
    };
    ensure_limit(
        normalized_retained_charge(&normalized).unwrap_or(usize::MAX),
        limit,
    )?;
    normalized.retained_bytes = required;
    Ok(normalized)
}

fn materialize_outcome(
    entry: &ResponseEntryData,
    required: usize,
    limit: usize,
) -> Result<NormalizedAlterClientQuotaOutcome, AlterClientQuotasResponseFailure> {
    let mut entity = Vec::new();
    entity
        .try_reserve_exact(entry.entity.len())
        .map_err(|_| retained_failure(required, limit))?;
    for component in &entry.entity {
        entity.push(NormalizedAlterClientQuotaEntityComponent {
            entity_type: copy_string(component.entity_type.as_str(), required, limit)?,
            entity_name: component
                .entity_name
                .as_deref()
                .map(|name| copy_string(name, required, limit))
                .transpose()?,
        });
    }
    entity.sort_unstable_by(|left, right| {
        left.entity_type
            .as_bytes()
            .cmp(right.entity_type.as_bytes())
            .then_with(|| left.entity_name.cmp(&right.entity_name))
    });
    let diagnostic_len = bounded_diagnostic_len(entry.error_message.as_deref());
    let error_message = entry
        .error_message
        .as_deref()
        .map(|message| copy_string(&message[..diagnostic_len], required, limit))
        .transpose()?;
    Ok(NormalizedAlterClientQuotaOutcome {
        entity,
        error_code: entry.error_code,
        error_message,
        error_message_truncated: entry
            .error_message
            .as_ref()
            .is_some_and(|message| diagnostic_len < message.len()),
    })
}

fn copy_string(
    source: &str,
    required: usize,
    limit: usize,
) -> Result<String, AlterClientQuotasResponseFailure> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(source.len())
        .map_err(|_| retained_failure(required, limit))?;
    owned.push_str(source);
    Ok(owned)
}

fn map_request_failure(
    failure: AlterClientQuotasRequestFailure,
    required: usize,
    limit: usize,
) -> AlterClientQuotasResponseFailure {
    match failure {
        AlterClientQuotasRequestFailure::RetainedBytes { .. } => retained_failure(required, limit),
        _ => AlterClientQuotasResponseFailure::InvalidRequest,
    }
}

fn ensure_limit(required: usize, limit: usize) -> Result<(), AlterClientQuotasResponseFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(AlterClientQuotasResponseFailure::RetainedBytes { required, limit })
}

const fn retained_failure(required: usize, limit: usize) -> AlterClientQuotasResponseFailure {
    AlterClientQuotasResponseFailure::RetainedBytes { required, limit }
}
