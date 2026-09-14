//! Generated API-key 33 request construction from one validated resource plan.

use kafka_client_core::LegacyAlterConfigsPlan;
use kafka_wire::{
    AlterConfigsRequest,
    alter_configs_request::{AlterConfigsResource, AlterableConfig},
};

/// Builds one full-snapshot resource request without fallback or transport authority.
///
/// Kafka restores a legacy snapshot key by omitting its dynamic override from
/// the complete replacement. The nullable model value is therefore a local
/// restoration directive, not a null value to transmit to the broker.
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
                .filter_map(|entry| {
                    let value = entry.value()?;
                    let mut config = AlterableConfig::default();
                    config.name = entry.key().into();
                    config.value = Some(value.into());
                    Some(config)
                })
                .collect();
            resource
        })
        .collect();
    request.validate_only = plan.validate_only();
    request
}
