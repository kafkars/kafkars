//! Fallible nested DeleteAcls value translation without infallible collection.

use crate::{
    DeliveryStatus, ErrorKind, KafkaError,
    admin::{
        AccessControlEntry, AclBinding, AclBindingFilter, AclOperation, AclPatternType,
        AclPermissionType, AclResourceType, DeleteAclBrokerError, DeleteAclFilterOutcome,
        DeleteAclFilterResult, DeleteAclMatchOutcome, DeleteAclMatchResult, ResourcePattern,
    },
};

use super::engine::{
    BrokerError, Filter, FilterOutcome, FilterResult, MatchResult, MatchingBinding,
};

pub(super) fn translate_filter_outcome(
    outcome: FilterOutcome,
) -> Result<DeleteAclFilterOutcome, KafkaError> {
    let (filter, result) = outcome.into_parts();
    Ok(DeleteAclFilterOutcome::new(
        translate_filter(filter),
        translate_filter_result(result)?,
    ))
}

fn translate_filter_result(result: FilterResult) -> Result<DeleteAclFilterResult, KafkaError> {
    match result {
        FilterResult::Matched(matches) => {
            let mut translated = Vec::new();
            translated
                .try_reserve_exact(matches.len())
                .map_err(|_error| nested_result_capacity_rejected())?;
            for matching in matches {
                translated.push(translate_matching(matching));
            }
            Ok(DeleteAclFilterResult::Matched(translated))
        }
        FilterResult::BrokerFailed(error) => {
            Ok(DeleteAclFilterResult::BrokerFailed(translate_broker(error)))
        }
    }
}

fn translate_filter(filter: Filter) -> AclBindingFilter {
    let (resource_type, resource_name, pattern_type, principal, host, operation, permission_type) =
        filter.into_parts();
    translate_filter_parts(
        resource_type,
        resource_name,
        pattern_type,
        principal,
        host,
        operation,
        permission_type,
    )
}

fn translate_matching(matching: MatchingBinding) -> DeleteAclMatchOutcome {
    let (
        resource_type,
        resource_name,
        pattern_type,
        principal,
        host,
        operation,
        permission,
        result,
    ) = matching.into_parts();
    DeleteAclMatchOutcome::new(
        binding(
            resource_type,
            resource_name,
            pattern_type,
            principal,
            host,
            operation,
            permission,
        ),
        match result {
            MatchResult::Deleted => DeleteAclMatchResult::Deleted,
            MatchResult::BrokerFailed(error) => {
                DeleteAclMatchResult::BrokerFailed(translate_broker(error))
            }
        },
    )
}

fn translate_broker(error: BrokerError) -> DeleteAclBrokerError {
    let (code, message, truncated) = error.into_parts();
    translate_broker_parts(code, message, truncated)
}

pub(super) fn translate_broker_parts(
    code: i16,
    message: Option<String>,
    truncated: bool,
) -> DeleteAclBrokerError {
    DeleteAclBrokerError::new(code, message, truncated)
}

fn nested_result_capacity_rejected() -> KafkaError {
    KafkaError::new(
        ErrorKind::Backpressure,
        "DeleteAcls nested public result capacity is unavailable",
    )
    .with_delivery_status(DeliveryStatus::PossiblySent)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn translate_filter_parts(
    resource_type: i8,
    resource_name: Option<String>,
    pattern_type: i8,
    principal: Option<String>,
    host: Option<String>,
    operation: i8,
    permission_type: i8,
) -> AclBindingFilter {
    let mut filter = AclBindingFilter::new(
        AclResourceType::from_code(resource_type),
        AclPatternType::from_code(pattern_type),
        AclOperation::from_code(operation),
        AclPermissionType::from_code(permission_type),
    );
    if let Some(resource_name) = resource_name {
        filter = filter.with_resource_name(resource_name);
    }
    if let Some(principal) = principal {
        filter = filter.with_principal(principal);
    }
    if let Some(host) = host {
        filter = filter.with_host(host);
    }
    filter
}

#[allow(clippy::too_many_arguments)]
fn binding(
    resource_type: i8,
    resource_name: String,
    pattern_type: i8,
    principal: String,
    host: String,
    operation: i8,
    permission_type: i8,
) -> AclBinding {
    AclBinding::new(
        ResourcePattern::new(
            AclResourceType::from_code(resource_type),
            resource_name,
            AclPatternType::from_code(pattern_type),
        ),
        AccessControlEntry::new(
            principal,
            host,
            AclOperation::from_code(operation),
            AclPermissionType::from_code(permission_type),
        ),
    )
}
