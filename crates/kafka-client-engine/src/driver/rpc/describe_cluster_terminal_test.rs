//! Semantic normalization scenarios for `DescribeCluster` call terminals.

use kafka_client_core::{DeliveryStatus, DescribeClusterInput};
use kafka_driver::{ApiKey, ApiVersion, RequestError};
use kafka_wire::DescribeClusterResponse;

use super::describe_cluster_terminal::normalize_terminal;

#[test]
fn valid_generated_response_becomes_one_bounded_core_fact() {
    let mut response = DescribeClusterResponse::default();
    response.endpoint_type = 1;
    response.cluster_id = "cluster-a".into();
    assert!(matches!(
        normalize_terminal(128 * 1024, false, false, Ok(response)),
        DescribeClusterInput::BrokerResponded { .. }
    ));
}

#[test]
fn over_budget_generated_response_becomes_invalid_response() {
    let mut response = DescribeClusterResponse::default();
    response.endpoint_type = 1;
    response.cluster_id = "cluster-a".into();
    assert_eq!(
        normalize_terminal(1, false, false, Ok(response)),
        DescribeClusterInput::InvalidResponse
    );
}

#[test]
fn unrequested_authorized_operations_becomes_invalid_response() {
    let mut response = DescribeClusterResponse::default();
    response.endpoint_type = 1;
    response.cluster_id = "cluster-a".into();
    response.cluster_authorized_operations = 0x21;
    assert_eq!(
        normalize_terminal(128 * 1024, false, false, Ok(response)),
        DescribeClusterInput::InvalidResponse
    );
}

#[test]
fn unavailable_request_version_is_definitely_unsent_compatibility() {
    assert_eq!(
        normalize_terminal(
            128 * 1024,
            true,
            false,
            Err(RequestError::VersionLimitUnavailable {
                api_key: ApiKey::new(60),
                maximum: ApiVersion::new(1),
                negotiated_minimum: ApiVersion::new(2),
            }),
        ),
        DescribeClusterInput::ProtocolIncompatible {
            delivery: DeliveryStatus::NotSent,
        }
    );
}
