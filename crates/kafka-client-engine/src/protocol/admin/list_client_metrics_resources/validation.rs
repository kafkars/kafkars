//! Allocation-free hostile-shape validation for API-key 74 v0.

use kafka_client_core::{
    LIST_CLIENT_METRICS_RESOURCES_MAX_RESOURCE_NAME_BYTES,
    LIST_CLIENT_METRICS_RESOURCES_MAX_RESOURCES, LIST_CLIENT_METRICS_RESOURCES_MAX_RETAINED_BYTES,
};
use kafka_wire::ListConfigResourcesResponse;

use super::ListClientMetricsResourcesProtocolFailure;

pub(super) const CLIENT_METRICS_RESOURCE_TYPE: i8 = 16;
pub(super) const MAX_RESOURCES: usize = LIST_CLIENT_METRICS_RESOURCES_MAX_RESOURCES;
pub(super) const MAX_RESOURCE_NAME_BYTES: usize =
    LIST_CLIENT_METRICS_RESOURCES_MAX_RESOURCE_NAME_BYTES;
pub(super) const MAX_RESPONSE_TEXT_BYTES: usize = LIST_CLIENT_METRICS_RESOURCES_MAX_RETAINED_BYTES;

pub(super) fn validate_response(
    response: &ListConfigResourcesResponse,
) -> Result<(), ListClientMetricsResourcesProtocolFailure> {
    if response.error_code != 0 {
        return response
            .config_resources
            .is_empty()
            .then_some(())
            .ok_or(ListClientMetricsResourcesProtocolFailure::SuccessPayloadWithBrokerError);
    }
    if response.config_resources.len() > MAX_RESOURCES {
        return Err(
            ListClientMetricsResourcesProtocolFailure::TooManyResources {
                actual: response.config_resources.len(),
                max: MAX_RESOURCES,
            },
        );
    }
    let mut text_bytes = 0usize;
    for resource in &response.config_resources {
        if resource.resource_type != CLIENT_METRICS_RESOURCE_TYPE {
            return Err(
                ListClientMetricsResourcesProtocolFailure::UnexpectedResourceType {
                    actual: resource.resource_type,
                },
            );
        }
        if resource.resource_name.is_empty() {
            return Err(ListClientMetricsResourcesProtocolFailure::EmptyResourceName);
        }
        if resource.resource_name.len() > MAX_RESOURCE_NAME_BYTES {
            return Err(
                ListClientMetricsResourcesProtocolFailure::ResourceNameTooLong {
                    actual: resource.resource_name.len(),
                    max: MAX_RESOURCE_NAME_BYTES,
                },
            );
        }
        text_bytes = text_bytes
            .checked_add(resource.resource_name.len())
            .unwrap_or(usize::MAX);
        if text_bytes > MAX_RESPONSE_TEXT_BYTES {
            return Err(
                ListClientMetricsResourcesProtocolFailure::ResponseTextBytesExceeded {
                    required: text_bytes,
                    max: MAX_RESPONSE_TEXT_BYTES,
                },
            );
        }
    }
    Ok(())
}
