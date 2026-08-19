//! Validate-first borrowed canonicalization of generated quota entries.

use kafka_wire::{
    DescribeClientQuotasResponse,
    describe_client_quotas_response::{EntityData, ValueData},
};

use super::{
    DescribeClientQuotasResponseFailure,
    retention::{
        CanonicalEntryRef, EntityComponentRef, MAX_ENTITY_COMPONENTS, MAX_ENTITY_NAME_BYTES,
        MAX_ENTITY_TYPE_BYTES, MAX_ENTRIES, MAX_QUOTA_KEY_BYTES, MAX_QUOTA_VALUES, QuotaValueRef,
    },
    version::supports_describe_client_quotas_version,
};

pub(super) fn validate_response_shape(
    selected_version: i16,
    response: &DescribeClientQuotasResponse,
) -> Result<(), DescribeClientQuotasResponseFailure> {
    validate_top_level(selected_version, response)?;
    validate_nested_shape(response)
}

fn validate_top_level(
    selected_version: i16,
    response: &DescribeClientQuotasResponse,
) -> Result<(), DescribeClientQuotasResponseFailure> {
    if !supports_describe_client_quotas_version(selected_version) {
        return Err(DescribeClientQuotasResponseFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    if response.throttle_time_ms < 0 {
        return Err(DescribeClientQuotasResponseFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        });
    }
    match (response.error_code, response.entries.as_ref()) {
        (0, None) => {
            return Err(DescribeClientQuotasResponseFailure::MissingEntriesOnSuccess);
        }
        (code, Some(entries)) if code != 0 && !entries.is_empty() => {
            return Err(
                DescribeClientQuotasResponseFailure::EntriesWithTopLevelError {
                    actual: entries.len(),
                },
            );
        }
        _ => {}
    }
    Ok(())
}

fn validate_nested_shape(
    response: &DescribeClientQuotasResponse,
) -> Result<(), DescribeClientQuotasResponseFailure> {
    let entries = response.entries.as_deref().unwrap_or_default();
    if entries.len() > MAX_ENTRIES {
        return Err(DescribeClientQuotasResponseFailure::TooManyEntries {
            actual: entries.len(),
            max: MAX_ENTRIES,
        });
    }
    let mut entity_count = 0usize;
    let mut value_count = 0usize;
    for entry in entries {
        if entry.entity.is_empty() {
            return Err(DescribeClientQuotasResponseFailure::EmptyEntity);
        }
        if entry.values.is_empty() {
            return Err(DescribeClientQuotasResponseFailure::EmptyValues);
        }
        entity_count = entity_count
            .checked_add(entry.entity.len())
            .unwrap_or(usize::MAX);
        if entity_count > MAX_ENTITY_COMPONENTS {
            return Err(
                DescribeClientQuotasResponseFailure::TooManyEntityComponents {
                    actual: entity_count,
                    max: MAX_ENTITY_COMPONENTS,
                },
            );
        }
        value_count = value_count
            .checked_add(entry.values.len())
            .unwrap_or(usize::MAX);
        if value_count > MAX_QUOTA_VALUES {
            return Err(DescribeClientQuotasResponseFailure::TooManyQuotaValues {
                actual: value_count,
                max: MAX_QUOTA_VALUES,
            });
        }
        for component in &entry.entity {
            validate_component(component)?;
        }
        for value in &entry.values {
            validate_value(value)?;
        }
    }
    Ok(())
}

fn validate_component(component: &EntityData) -> Result<(), DescribeClientQuotasResponseFailure> {
    if component.entity_type.is_empty() {
        return Err(DescribeClientQuotasResponseFailure::EmptyEntityType);
    }
    if component.entity_type.len() > MAX_ENTITY_TYPE_BYTES {
        return Err(DescribeClientQuotasResponseFailure::EntityTypeTooLong {
            actual: component.entity_type.len(),
            max: MAX_ENTITY_TYPE_BYTES,
        });
    }
    if let Some(name) = &component.entity_name {
        if name.is_empty() {
            return Err(DescribeClientQuotasResponseFailure::EmptyEntityName);
        }
        if name.len() > MAX_ENTITY_NAME_BYTES {
            return Err(DescribeClientQuotasResponseFailure::EntityNameTooLong {
                actual: name.len(),
                max: MAX_ENTITY_NAME_BYTES,
            });
        }
    }
    Ok(())
}

fn validate_value(value: &ValueData) -> Result<(), DescribeClientQuotasResponseFailure> {
    if value.key.is_empty() {
        return Err(DescribeClientQuotasResponseFailure::EmptyQuotaKey);
    }
    if value.key.len() > MAX_QUOTA_KEY_BYTES {
        return Err(DescribeClientQuotasResponseFailure::QuotaKeyTooLong {
            actual: value.key.len(),
            max: MAX_QUOTA_KEY_BYTES,
        });
    }
    if !value.value.is_finite() {
        return Err(DescribeClientQuotasResponseFailure::NonFiniteQuotaValue);
    }
    Ok(())
}

pub(super) fn canonicalize(
    entries: &[kafka_wire::describe_client_quotas_response::EntryData],
    required: usize,
    limit: usize,
) -> Result<Vec<CanonicalEntryRef<'_>>, DescribeClientQuotasResponseFailure> {
    let mut canonical = Vec::new();
    canonical
        .try_reserve_exact(entries.len())
        .map_err(|_| retained_failure(required, limit))?;
    for entry in entries {
        canonical.push(canonical_entry(entry, required, limit)?);
    }
    canonical.sort_unstable_by(|left, right| left.entity.cmp(&right.entity));
    if canonical
        .windows(2)
        .any(|pair| pair[0].entity == pair[1].entity)
    {
        return Err(DescribeClientQuotasResponseFailure::DuplicateEntity);
    }
    Ok(canonical)
}

fn canonical_entry(
    entry: &kafka_wire::describe_client_quotas_response::EntryData,
    required: usize,
    limit: usize,
) -> Result<CanonicalEntryRef<'_>, DescribeClientQuotasResponseFailure> {
    let mut entity = Vec::new();
    entity
        .try_reserve_exact(entry.entity.len())
        .map_err(|_| retained_failure(required, limit))?;
    entity.extend(entry.entity.iter().map(|component| EntityComponentRef {
        entity_type: component.entity_type.as_str(),
        entity_name: component.entity_name.as_deref(),
    }));
    entity.sort_unstable();
    if entity
        .windows(2)
        .any(|pair| pair[0].entity_type == pair[1].entity_type)
    {
        return Err(DescribeClientQuotasResponseFailure::DuplicateEntityType);
    }

    let mut values = Vec::new();
    values
        .try_reserve_exact(entry.values.len())
        .map_err(|_| retained_failure(required, limit))?;
    values.extend(entry.values.iter().map(|value| QuotaValueRef {
        key: value.key.as_str(),
        value: value.value,
    }));
    values.sort_unstable_by(|left, right| left.key.as_bytes().cmp(right.key.as_bytes()));
    if values.windows(2).any(|pair| pair[0].key == pair[1].key) {
        return Err(DescribeClientQuotasResponseFailure::DuplicateQuotaKey);
    }
    Ok(CanonicalEntryRef { entity, values })
}

const fn retained_failure(required: usize, limit: usize) -> DescribeClientQuotasResponseFailure {
    DescribeClientQuotasResponseFailure::RetainedBytes { required, limit }
}
