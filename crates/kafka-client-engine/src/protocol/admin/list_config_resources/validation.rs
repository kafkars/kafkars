//! Allocation-free hostile-shape validation for successful API-key 74 v1 responses.

use kafka_wire::ListConfigResourcesResponse;

use super::ListConfigResourcesProtocolFailure;

pub(super) const MAX_RESOURCES: usize = 4_096;
pub(super) const MAX_RESOURCE_NAME_BYTES: usize = 256;
pub(super) const MAX_RESPONSE_TEXT_BYTES: usize = 1024 * 1024;

pub(super) fn validate_success_response(
    response: &ListConfigResourcesResponse,
) -> Result<(), ListConfigResourcesProtocolFailure> {
    if response.config_resources.len() > MAX_RESOURCES {
        return Err(ListConfigResourcesProtocolFailure::TooManyResources {
            actual: response.config_resources.len(),
            max: MAX_RESOURCES,
        });
    }
    let mut text_bytes = 0usize;
    for resource in &response.config_resources {
        if resource.resource_type <= 0 {
            return Err(
                ListConfigResourcesProtocolFailure::NonPositiveResourceType {
                    actual: resource.resource_type,
                },
            );
        }
        let name = resource.resource_name.as_str();
        if name.is_empty() {
            return Err(ListConfigResourcesProtocolFailure::EmptyResourceName);
        }
        if name.len() > MAX_RESOURCE_NAME_BYTES {
            return Err(ListConfigResourcesProtocolFailure::ResourceNameTooLong {
                actual: name.len(),
                max: MAX_RESOURCE_NAME_BYTES,
            });
        }
        text_bytes = text_bytes.checked_add(name.len()).unwrap_or(usize::MAX);
        if text_bytes > MAX_RESPONSE_TEXT_BYTES {
            return Err(
                ListConfigResourcesProtocolFailure::ResponseTextBytesExceeded {
                    required: text_bytes,
                    max: MAX_RESPONSE_TEXT_BYTES,
                },
            );
        }
    }
    Ok(())
}
