//! Borrowed validation and canonicalization of returned quota identities.

use kafka_wire::alter_client_quotas_response::EntryData as ResponseEntryData;

use super::{
    AlterClientQuotasResponseFailure,
    model::{CanonicalEntityComponentRef, CanonicalEntityRef},
    retention::{MAX_ENTITY_COMPONENTS, MAX_ENTITY_NAME_BYTES, MAX_ENTITY_TYPE_BYTES},
};

#[derive(Debug)]
pub(super) struct CanonicalResponseEntryRef<'a> {
    pub(super) entity: CanonicalEntityRef<'a>,
    pub(super) source: &'a ResponseEntryData,
}

pub(super) fn canonicalize_response_entries(
    entries: &[ResponseEntryData],
    required: usize,
    limit: usize,
) -> Result<Vec<CanonicalResponseEntryRef<'_>>, AlterClientQuotasResponseFailure> {
    let mut canonical = Vec::new();
    canonical
        .try_reserve_exact(entries.len())
        .map_err(|_| retained(required, limit))?;
    for entry in entries {
        canonical.push(CanonicalResponseEntryRef {
            entity: canonicalize_entity(&entry.entity, required, limit)?,
            source: entry,
        });
    }
    Ok(canonical)
}

fn canonicalize_entity(
    entity: &[kafka_wire::alter_client_quotas_response::EntityData],
    required: usize,
    limit: usize,
) -> Result<CanonicalEntityRef<'_>, AlterClientQuotasResponseFailure> {
    if entity.is_empty() {
        return Err(AlterClientQuotasResponseFailure::EmptyEntity);
    }
    if entity.len() > MAX_ENTITY_COMPONENTS {
        return Err(AlterClientQuotasResponseFailure::TooManyEntityComponents {
            actual: entity.len(),
            max: MAX_ENTITY_COMPONENTS,
        });
    }
    let mut components = Vec::new();
    components
        .try_reserve_exact(entity.len())
        .map_err(|_| retained(required, limit))?;
    for component in entity {
        validate_component(component)?;
        components.push(CanonicalEntityComponentRef {
            entity_type: component.entity_type.as_str(),
            entity_name: component.entity_name.as_deref(),
        });
    }
    components.sort_unstable();
    if components
        .windows(2)
        .any(|pair| pair[0].entity_type == pair[1].entity_type)
    {
        return Err(AlterClientQuotasResponseFailure::DuplicateEntityType);
    }
    Ok(CanonicalEntityRef { components })
}

fn validate_component(
    component: &kafka_wire::alter_client_quotas_response::EntityData,
) -> Result<(), AlterClientQuotasResponseFailure> {
    validate_text(
        component.entity_type.as_str(),
        AlterClientQuotasResponseFailure::EmptyEntityType,
        |actual| AlterClientQuotasResponseFailure::EntityTypeTooLong {
            actual,
            max: MAX_ENTITY_TYPE_BYTES,
        },
        MAX_ENTITY_TYPE_BYTES,
    )?;
    if let Some(name) = component.entity_name.as_deref() {
        validate_text(
            name,
            AlterClientQuotasResponseFailure::EmptyEntityName,
            |actual| AlterClientQuotasResponseFailure::EntityNameTooLong {
                actual,
                max: MAX_ENTITY_NAME_BYTES,
            },
            MAX_ENTITY_NAME_BYTES,
        )?;
    }
    Ok(())
}

fn validate_text(
    value: &str,
    empty: AlterClientQuotasResponseFailure,
    too_long: impl FnOnce(usize) -> AlterClientQuotasResponseFailure,
    max: usize,
) -> Result<(), AlterClientQuotasResponseFailure> {
    if value.is_empty() {
        return Err(empty);
    }
    if value.len() > max {
        return Err(too_long(value.len()));
    }
    Ok(())
}

const fn retained(required: usize, limit: usize) -> AlterClientQuotasResponseFailure {
    AlterClientQuotasResponseFailure::RetainedBytes { required, limit }
}
