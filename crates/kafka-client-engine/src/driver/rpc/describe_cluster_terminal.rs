//! Semantic terminal normalization for plain `DescribeCluster` calls.

use kafka_client_core::DescribeClusterInput;
use kafka_driver::{CallFailure, ConnectionCloseReason, RequestError};
use kafka_wire::DescribeClusterResponse;

use crate::protocol::admin::describe_cluster::normalize_describe_cluster_response;

pub(super) fn normalize_terminal(
    retained_bytes: usize,
    include_fenced_brokers: bool,
    include_authorized_operations: bool,
    result: Result<DescribeClusterResponse, RequestError>,
) -> DescribeClusterInput {
    match result {
        Ok(response) => normalize_describe_cluster_response(
            &response,
            retained_bytes,
            include_fenced_brokers,
            include_authorized_operations,
        )
        .unwrap_or(DescribeClusterInput::InvalidResponse),
        Err(error) if is_compatibility_failure(&error) => {
            DescribeClusterInput::ProtocolIncompatible {
                delivery: super::super::request_failure_delivery(&error),
            }
        }
        Err(error) if is_authentication_failure(&error) => {
            DescribeClusterInput::AuthenticationFailed {
                delivery: super::super::request_failure_delivery(&error),
            }
        }
        Err(error) => {
            if matches!(
                &error,
                RequestError::Rejected {
                    failure: CallFailure::CorrelationMismatch { .. },
                    ..
                } | RequestError::Decode(_)
            ) {
                DescribeClusterInput::InvalidResponse
            } else {
                DescribeClusterInput::TransportFailed {
                    delivery: super::super::request_failure_delivery(&error),
                }
            }
        }
    }
}

const fn is_compatibility_failure(error: &RequestError) -> bool {
    matches!(
        error,
        RequestError::Encode(_)
            | RequestError::UnsupportedVersion { .. }
            | RequestError::ApiUnavailable { .. }
            | RequestError::VersionLimitUnavailable { .. }
            | RequestError::VersionFloorUnavailable { .. }
            | RequestError::VersionBoundsInvalid { .. }
    )
}

const fn is_authentication_failure(error: &RequestError) -> bool {
    matches!(
        error,
        RequestError::Rejected {
            failure: CallFailure::ConnectionClosed {
                reason: ConnectionCloseReason::AuthenticationFailed(_),
            },
            ..
        }
    )
}
