//! Bounded construction of one canonical generated client-quota alteration.

use kafka_wire::{
    AlterClientQuotasRequest, RetainedSize,
    alter_client_quotas_request::{EntityData, EntryData, OpData},
};

use super::{
    AlterClientQuotaOperationKindRef, AlterClientQuotasRequestRef,
    request_validation::{CanonicalAlterationRef, canonicalize_request},
    retention::request_peak_charge,
};

/// Invalid alteration shape or insufficient capacity before driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterClientQuotasRequestFailure {
    EmptyAlterations,
    TooManyAlterations { actual: usize, max: usize },
    EmptyEntity,
    TooManyEntityComponents { actual: usize, max: usize },
    EmptyOperations,
    TooManyOperations { actual: usize, max: usize },
    EmptyEntityType,
    EntityTypeTooLong { actual: usize, max: usize },
    EmptyEntityName,
    EntityNameTooLong { actual: usize, max: usize },
    EmptyQuotaKey,
    QuotaKeyTooLong { actual: usize, max: usize },
    NonFiniteQuotaValue,
    DuplicateEntityType,
    DuplicateQuotaKey,
    DuplicateEntity,
    RetainedBytes { required: usize, limit: usize },
}

/// Builds API-key 49 without acquiring route, deadline, or completion authority.
pub(crate) fn alter_client_quotas_request(
    source: AlterClientQuotasRequestRef<'_>,
    retained_limit: usize,
) -> Result<AlterClientQuotasRequest, AlterClientQuotasRequestFailure> {
    let required = request_peak_charge(source).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;
    let canonical = canonicalize_request(source, required, retained_limit)?;

    let mut entries = Vec::new();
    entries
        .try_reserve_exact(canonical.len())
        .map_err(|_| retained_failure(required, retained_limit))?;
    for alteration in canonical {
        entries.push(generated_entry(alteration, required, retained_limit)?);
    }
    let mut request = AlterClientQuotasRequest::default();
    request.entries = entries;
    request.validate_only = source.validate_only();
    ensure_limit(request.retained_size().heap_bytes(), retained_limit)?;
    Ok(request)
}

fn generated_entry(
    alteration: CanonicalAlterationRef<'_>,
    required: usize,
    limit: usize,
) -> Result<EntryData, AlterClientQuotasRequestFailure> {
    let mut entity = Vec::new();
    entity
        .try_reserve_exact(alteration.entity.components.len())
        .map_err(|_| retained_failure(required, limit))?;
    for component in alteration.entity.components {
        let mut generated = EntityData::default();
        generated.entity_type = copy_string(component.entity_type, required, limit)?.into();
        generated.entity_name = component
            .entity_name
            .map(|name| copy_string(name, required, limit).map(Into::into))
            .transpose()?;
        entity.push(generated);
    }

    let mut ops = Vec::new();
    ops.try_reserve_exact(alteration.operations.len())
        .map_err(|_| retained_failure(required, limit))?;
    for operation in alteration.operations {
        let (value, remove) = match operation.kind() {
            AlterClientQuotaOperationKindRef::Set(value) => (value, false),
            AlterClientQuotaOperationKindRef::Remove => (0.0, true),
        };
        let mut generated = OpData::default();
        generated.key = copy_string(operation.key(), required, limit)?.into();
        generated.value = value;
        generated.remove = remove;
        ops.push(generated);
    }
    let mut entry = EntryData::default();
    entry.entity = entity;
    entry.ops = ops;
    Ok(entry)
}

fn copy_string(
    source: &str,
    required: usize,
    limit: usize,
) -> Result<String, AlterClientQuotasRequestFailure> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(source.len())
        .map_err(|_| retained_failure(required, limit))?;
    owned.push_str(source);
    Ok(owned)
}

fn ensure_limit(required: usize, limit: usize) -> Result<(), AlterClientQuotasRequestFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(AlterClientQuotasRequestFailure::RetainedBytes { required, limit })
}

const fn retained_failure(required: usize, limit: usize) -> AlterClientQuotasRequestFailure {
    AlterClientQuotasRequestFailure::RetainedBytes { required, limit }
}
