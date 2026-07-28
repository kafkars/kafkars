//! Fallible bounded construction of one generated client-quota filter request.

use kafka_wire::{
    DescribeClientQuotasRequest, RetainedSize, describe_client_quotas_request::ComponentData,
};

use super::{
    DescribeClientQuotaFilterComponentRef, DescribeClientQuotaMatchRef,
    DescribeClientQuotasFilterRef,
    retention::{
        MAX_ENTITY_NAME_BYTES, MAX_ENTITY_TYPE_BYTES, MAX_FILTER_COMPONENTS, request_peak_charge,
    },
};

/// Invalid filter shape or insufficient capacity before driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeClientQuotasRequestFailure {
    TooManyComponents { actual: usize, max: usize },
    EmptyEntityType,
    EntityTypeTooLong { actual: usize, max: usize },
    EmptyExactMatch,
    ExactMatchTooLong { actual: usize, max: usize },
    DuplicateEntityType,
    RetainedBytes { required: usize, limit: usize },
}

/// Builds API-key 48 without acquiring route, deadline, or completion authority.
pub(crate) fn describe_client_quotas_request(
    filter: DescribeClientQuotasFilterRef<'_>,
    retained_limit: usize,
) -> Result<DescribeClientQuotasRequest, DescribeClientQuotasRequestFailure> {
    validate_shape(filter.components())?;
    let required = request_peak_charge(filter.components()).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;
    validate_unique_types(filter.components(), required, retained_limit)?;

    let mut components = Vec::new();
    components
        .try_reserve_exact(filter.components().len())
        .map_err(|_| retained_failure(required, retained_limit))?;
    for component in filter.components() {
        components.push(generated_component(*component, required, retained_limit)?);
    }
    let mut request = DescribeClientQuotasRequest::default();
    request.components = components;
    request.strict = filter.strict();
    ensure_limit(request.retained_size().heap_bytes(), retained_limit)?;
    Ok(request)
}

fn validate_shape(
    components: &[DescribeClientQuotaFilterComponentRef<'_>],
) -> Result<(), DescribeClientQuotasRequestFailure> {
    if components.len() > MAX_FILTER_COMPONENTS {
        return Err(DescribeClientQuotasRequestFailure::TooManyComponents {
            actual: components.len(),
            max: MAX_FILTER_COMPONENTS,
        });
    }
    for component in components {
        validate_text(
            component.entity_type(),
            DescribeClientQuotasRequestFailure::EmptyEntityType,
            |actual| DescribeClientQuotasRequestFailure::EntityTypeTooLong {
                actual,
                max: MAX_ENTITY_TYPE_BYTES,
            },
            MAX_ENTITY_TYPE_BYTES,
        )?;
        if let DescribeClientQuotaMatchRef::Exact(value) = component.match_() {
            validate_text(
                value,
                DescribeClientQuotasRequestFailure::EmptyExactMatch,
                |actual| DescribeClientQuotasRequestFailure::ExactMatchTooLong {
                    actual,
                    max: MAX_ENTITY_NAME_BYTES,
                },
                MAX_ENTITY_NAME_BYTES,
            )?;
        }
    }
    Ok(())
}

fn validate_unique_types(
    components: &[DescribeClientQuotaFilterComponentRef<'_>],
    required: usize,
    limit: usize,
) -> Result<(), DescribeClientQuotasRequestFailure> {
    let mut types = Vec::new();
    types
        .try_reserve_exact(components.len())
        .map_err(|_| retained_failure(required, limit))?;
    types.extend(components.iter().map(|component| component.entity_type()));
    types.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    if types.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(DescribeClientQuotasRequestFailure::DuplicateEntityType);
    }
    Ok(())
}

fn generated_component(
    component: DescribeClientQuotaFilterComponentRef<'_>,
    required: usize,
    limit: usize,
) -> Result<ComponentData, DescribeClientQuotasRequestFailure> {
    let (match_type, match_) = match component.match_() {
        DescribeClientQuotaMatchRef::Exact(value) => {
            (0, Some(copy_string(value, required, limit)?.into()))
        }
        DescribeClientQuotaMatchRef::Default => (1, None),
        DescribeClientQuotaMatchRef::AnySpecified => (2, None),
    };
    let mut generated = ComponentData::default();
    generated.entity_type = copy_string(component.entity_type(), required, limit)?.into();
    generated.match_type = match_type;
    generated.match_ = match_;
    Ok(generated)
}

fn validate_text(
    value: &str,
    empty: DescribeClientQuotasRequestFailure,
    too_long: impl FnOnce(usize) -> DescribeClientQuotasRequestFailure,
    max: usize,
) -> Result<(), DescribeClientQuotasRequestFailure> {
    if value.is_empty() {
        return Err(empty);
    }
    if value.len() > max {
        return Err(too_long(value.len()));
    }
    Ok(())
}

fn copy_string(
    source: &str,
    required: usize,
    limit: usize,
) -> Result<String, DescribeClientQuotasRequestFailure> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(source.len())
        .map_err(|_| retained_failure(required, limit))?;
    owned.push_str(source);
    Ok(owned)
}

fn ensure_limit(required: usize, limit: usize) -> Result<(), DescribeClientQuotasRequestFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(DescribeClientQuotasRequestFailure::RetainedBytes { required, limit })
}

const fn retained_failure(required: usize, limit: usize) -> DescribeClientQuotasRequestFailure {
    DescribeClientQuotasRequestFailure::RetainedBytes { required, limit }
}
