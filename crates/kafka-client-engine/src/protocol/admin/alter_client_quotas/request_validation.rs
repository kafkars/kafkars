//! Borrowed canonicalization and duplicate detection for alteration intent.

use super::{
    AlterClientQuotaAlterationRef, AlterClientQuotaEntityComponentRef,
    AlterClientQuotaOperationKindRef, AlterClientQuotaOperationRef,
    AlterClientQuotasRequestFailure, AlterClientQuotasRequestRef,
    model::{CanonicalEntityComponentRef, CanonicalEntityRef},
    retention::{
        MAX_ALTERATIONS, MAX_ENTITY_COMPONENTS, MAX_ENTITY_NAME_BYTES, MAX_ENTITY_TYPE_BYTES,
        MAX_OPERATIONS, MAX_QUOTA_KEY_BYTES,
    },
};

#[derive(Debug, PartialEq)]
pub(super) struct CanonicalAlterationRef<'a> {
    pub(super) entity: CanonicalEntityRef<'a>,
    pub(super) operations: Vec<AlterClientQuotaOperationRef<'a>>,
}

pub(super) fn canonicalize_request(
    request: AlterClientQuotasRequestRef<'_>,
    required: usize,
    limit: usize,
) -> Result<Vec<CanonicalAlterationRef<'_>>, AlterClientQuotasRequestFailure> {
    validate_request_count(request.alterations())?;
    let mut canonical = Vec::new();
    canonical
        .try_reserve_exact(request.alterations().len())
        .map_err(|_| retained(required, limit))?;
    for alteration in request.alterations() {
        canonical.push(canonicalize_alteration(*alteration, required, limit)?);
    }
    reject_duplicate_entities(&canonical, required, limit)?;
    Ok(canonical)
}

fn validate_request_count(
    alterations: &[AlterClientQuotaAlterationRef<'_>],
) -> Result<(), AlterClientQuotasRequestFailure> {
    if alterations.is_empty() {
        return Err(AlterClientQuotasRequestFailure::EmptyAlterations);
    }
    if alterations.len() > MAX_ALTERATIONS {
        return Err(AlterClientQuotasRequestFailure::TooManyAlterations {
            actual: alterations.len(),
            max: MAX_ALTERATIONS,
        });
    }
    Ok(())
}

fn canonicalize_alteration(
    alteration: AlterClientQuotaAlterationRef<'_>,
    required: usize,
    limit: usize,
) -> Result<CanonicalAlterationRef<'_>, AlterClientQuotasRequestFailure> {
    validate_entity_count(alteration.entity())?;
    validate_operation_count(alteration.operations())?;
    let entity = canonicalize_entity(alteration.entity(), required, limit)?;
    let operations = canonicalize_operations(alteration.operations(), required, limit)?;
    Ok(CanonicalAlterationRef { entity, operations })
}

fn validate_entity_count(
    entity: &[AlterClientQuotaEntityComponentRef<'_>],
) -> Result<(), AlterClientQuotasRequestFailure> {
    if entity.is_empty() {
        return Err(AlterClientQuotasRequestFailure::EmptyEntity);
    }
    if entity.len() > MAX_ENTITY_COMPONENTS {
        return Err(AlterClientQuotasRequestFailure::TooManyEntityComponents {
            actual: entity.len(),
            max: MAX_ENTITY_COMPONENTS,
        });
    }
    Ok(())
}

fn validate_operation_count(
    operations: &[AlterClientQuotaOperationRef<'_>],
) -> Result<(), AlterClientQuotasRequestFailure> {
    if operations.is_empty() {
        return Err(AlterClientQuotasRequestFailure::EmptyOperations);
    }
    if operations.len() > MAX_OPERATIONS {
        return Err(AlterClientQuotasRequestFailure::TooManyOperations {
            actual: operations.len(),
            max: MAX_OPERATIONS,
        });
    }
    Ok(())
}

fn canonicalize_entity<'a>(
    entity: &[AlterClientQuotaEntityComponentRef<'a>],
    required: usize,
    limit: usize,
) -> Result<CanonicalEntityRef<'a>, AlterClientQuotasRequestFailure> {
    let mut components = Vec::new();
    components
        .try_reserve_exact(entity.len())
        .map_err(|_| retained(required, limit))?;
    for component in entity {
        validate_component(*component)?;
        components.push(CanonicalEntityComponentRef {
            entity_type: component.entity_type(),
            entity_name: component.entity_name(),
        });
    }
    components.sort_unstable();
    if components
        .windows(2)
        .any(|pair| pair[0].entity_type == pair[1].entity_type)
    {
        return Err(AlterClientQuotasRequestFailure::DuplicateEntityType);
    }
    Ok(CanonicalEntityRef { components })
}

fn validate_component(
    component: AlterClientQuotaEntityComponentRef<'_>,
) -> Result<(), AlterClientQuotasRequestFailure> {
    validate_text(
        component.entity_type(),
        AlterClientQuotasRequestFailure::EmptyEntityType,
        |actual| AlterClientQuotasRequestFailure::EntityTypeTooLong {
            actual,
            max: MAX_ENTITY_TYPE_BYTES,
        },
        MAX_ENTITY_TYPE_BYTES,
    )?;
    if let Some(name) = component.entity_name() {
        validate_text(
            name,
            AlterClientQuotasRequestFailure::EmptyEntityName,
            |actual| AlterClientQuotasRequestFailure::EntityNameTooLong {
                actual,
                max: MAX_ENTITY_NAME_BYTES,
            },
            MAX_ENTITY_NAME_BYTES,
        )?;
    }
    Ok(())
}

fn canonicalize_operations<'a>(
    source: &[AlterClientQuotaOperationRef<'a>],
    required: usize,
    limit: usize,
) -> Result<Vec<AlterClientQuotaOperationRef<'a>>, AlterClientQuotasRequestFailure> {
    let mut operations = Vec::new();
    operations
        .try_reserve_exact(source.len())
        .map_err(|_| retained(required, limit))?;
    for operation in source {
        validate_operation(*operation)?;
        operations.push(*operation);
    }
    let mut keys = Vec::new();
    keys.try_reserve_exact(source.len())
        .map_err(|_| retained(required, limit))?;
    keys.extend(source.iter().map(|operation| operation.key()));
    keys.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    if keys.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(AlterClientQuotasRequestFailure::DuplicateQuotaKey);
    }
    Ok(operations)
}

fn validate_operation(
    operation: AlterClientQuotaOperationRef<'_>,
) -> Result<(), AlterClientQuotasRequestFailure> {
    validate_text(
        operation.key(),
        AlterClientQuotasRequestFailure::EmptyQuotaKey,
        |actual| AlterClientQuotasRequestFailure::QuotaKeyTooLong {
            actual,
            max: MAX_QUOTA_KEY_BYTES,
        },
        MAX_QUOTA_KEY_BYTES,
    )?;
    if matches!(
        operation.kind(),
        AlterClientQuotaOperationKindRef::Set(value) if !value.is_finite()
    ) {
        return Err(AlterClientQuotasRequestFailure::NonFiniteQuotaValue);
    }
    Ok(())
}

fn reject_duplicate_entities(
    alterations: &[CanonicalAlterationRef<'_>],
    required: usize,
    limit: usize,
) -> Result<(), AlterClientQuotasRequestFailure> {
    let mut entities = Vec::new();
    entities
        .try_reserve_exact(alterations.len())
        .map_err(|_| retained(required, limit))?;
    entities.extend(alterations.iter().map(|alteration| &alteration.entity));
    entities.sort_unstable();
    if entities.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(AlterClientQuotasRequestFailure::DuplicateEntity);
    }
    Ok(())
}

fn validate_text<E>(
    value: &str,
    empty: E,
    too_long: impl FnOnce(usize) -> E,
    max: usize,
) -> Result<(), E> {
    if value.is_empty() {
        return Err(empty);
    }
    if value.len() > max {
        return Err(too_long(value.len()));
    }
    Ok(())
}

const fn retained(required: usize, limit: usize) -> AlterClientQuotasRequestFailure {
    AlterClientQuotasRequestFailure::RetainedBytes { required, limit }
}
