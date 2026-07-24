//! Generated v0/v1 request construction from one validated semantic plan.

use kafka_client_core::{ConfigAlterationOperation, IncrementalAlterConfigsPlan};
use kafka_wire::{
    IncrementalAlterConfigsRequest,
    incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig},
};

use super::resource::TOPIC_RESOURCE_TYPE;

/// Builds API-key 44 input without transport, retry, or fallback authority.
pub(crate) fn incremental_alter_configs_request(
    plan: &IncrementalAlterConfigsPlan,
) -> IncrementalAlterConfigsRequest {
    let mut request = IncrementalAlterConfigsRequest::default();
    request.resources = plan
        .topics()
        .iter()
        .map(|topic| {
            let mut resource = AlterConfigsResource::default();
            resource.resource_type = TOPIC_RESOURCE_TYPE;
            resource.resource_name = topic.topic().into();
            resource.configs = topic
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
