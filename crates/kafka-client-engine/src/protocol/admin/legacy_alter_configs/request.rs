//! Generated API-key 33 request construction from one validated resource plan.

use kafka_client_core::LegacyAlterConfigsPlan;
use kafka_wire::{
    AlterConfigsRequest,
    alter_configs_request::{AlterConfigsResource, AlterableConfig},
};

/// Builds one full-snapshot resource request without fallback or transport authority.
pub(crate) fn legacy_alter_configs_request(plan: &LegacyAlterConfigsPlan) -> AlterConfigsRequest {
    let mut request = AlterConfigsRequest::default();
    request.resources = plan
        .resources()
        .iter()
        .map(|planned| {
            let mut resource = AlterConfigsResource::default();
            resource.resource_type = planned.resource_type();
            resource.resource_name = planned.resource_name().into();
            resource.configs = planned
                .configs()
                .iter()
                .map(|entry| {
                    let mut config = AlterableConfig::default();
                    config.name = entry.key().into();
                    config.value = entry.value().map(Into::into);
                    config
                })
                .collect();
            resource
        })
        .collect();
    request.validate_only = plan.validate_only();
    request
}
