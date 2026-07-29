//! Explicit nonempty client identity for feature-bearing ApiVersions requests.

use kafka_wire::ApiVersionsRequest;

const CLIENT_SOFTWARE_NAME: &str = "kafka-client-rs";
const CLIENT_SOFTWARE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Builds a v3-v5-compatible feature query without invented cluster identity.
pub(crate) fn describe_features_request() -> ApiVersionsRequest {
    let mut request = ApiVersionsRequest::default();
    request.client_software_name = CLIENT_SOFTWARE_NAME.into();
    request.client_software_version = CLIENT_SOFTWARE_VERSION.into();
    request
}
