//! Generated v0/v1 request construction from one validated resource plan.

use kafka_client_core::{ConfigAlterationOperation, IncrementalAlterConfigsPlan};
use kafka_wire::{
    IncrementalAlterConfigsRequest,
    incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig},
};

/// Builds API-key 44 input without transport, retry, or fallback authority.
pub(crate) fn incremental_alter_configs_request(
    plan: &IncrementalAlterConfigsPlan,
) -> IncrementalAlterConfigsRequest {
    let mut request = IncrementalAlterConfigsRequest::default();
    request.resources = plan
        .resources()
        .iter()
        .map(|planned| {
            let mut resource = AlterConfigsResource::default();
            resource.resource_type = planned.resource_type();
            resource.resource_name = planned.resource_name().into();
            resource.configs = planned
                .alterations()
                .iter()
                .map(|alteration| {
                    let mut config = AlterableConfig::default();
                    config.name = alteration.key().into();
                    config.config_operation = operation_code(alteration.operation());
                    config.value = alteration.operation().value().map(Into::into);
                    config
                })
                .collect();
            resource
        })
        .collect();
    request.validate_only = plan.validate_only();
    request
}

const fn operation_code(operation: &ConfigAlterationOperation) -> i8 {
    match operation {
        ConfigAlterationOperation::Set(_) => 0,
        ConfigAlterationOperation::Delete => 1,
        ConfigAlterationOperation::Append(_) => 2,
        ConfigAlterationOperation::Subtract(_) => 3,
    }
}
