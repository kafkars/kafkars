//! Generated `DescribeCluster` request options and bounded response normalization.

use core::num::NonZeroI16;

use kafka_client_core::{
    ClusterBroker, ClusterDescription, DescribeClusterBrokerError, DescribeClusterInput,
};
use kafka_wire::{DescribeClusterRequest, DescribeClusterResponse};

const BROKER_ENDPOINT_TYPE: i8 = 1;
const MAX_BROKERS: usize = 256;
const MAX_CLUSTER_ID_BYTES: usize = 1024;
const MAX_HOST_BYTES: usize = 4096;
const MAX_RACK_BYTES: usize = 1024;
const MAX_DIAGNOSTIC_BYTES: usize = 1024;
const BASE_RESULT_BYTES: usize = 8 * 1024;
const BROKER_OWNER_BYTES: usize = 256;
const ENGINE_TEXT_COPIES: usize = 2;

/// Structural or retained-budget failure without hostile broker-owned strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DescribeClusterProtocolFailure {
    BrokerCapacity,
    ClusterIdBytes,
    HostBytes,
    RackBytes,
    BrokerId,
    DuplicateBrokerId,
    BrokerPort,
    EmptyHost,
    FencedBroker,
    ControllerId,
    EndpointType,
    AuthorizedOperations,
    RetainedBytes,
}

/// Builds one broker-endpoint request with explicit optional response expansion.
pub(crate) fn describe_cluster_request(
    include_fenced_brokers: bool,
    include_authorized_operations: bool,
) -> DescribeClusterRequest {
    let mut request = DescribeClusterRequest::default();
    request.include_cluster_authorized_operations = include_authorized_operations;
    request.endpoint_type = BROKER_ENDPOINT_TYPE;
    request.include_fenced_brokers = include_fenced_brokers;
    request
}

/// Converts one generated response without retaining unbounded broker text.
pub(crate) fn normalize_describe_cluster_response(
    response: &DescribeClusterResponse,
    retained_bytes: usize,
    include_fenced_brokers: bool,
    include_authorized_operations: bool,
) -> Result<DescribeClusterInput, DescribeClusterProtocolFailure> {
    if let Some(code) = NonZeroI16::new(response.error_code) {
        let (message, truncated) = bounded_diagnostic(response.error_message.as_deref());
        return Ok(DescribeClusterInput::BrokerRejected {
            error: DescribeClusterBrokerError::new(code, message, truncated),
        });
    }
    validate_success(
        response,
        retained_bytes,
        include_fenced_brokers,
        include_authorized_operations,
    )?;
    let controller_id = match response.controller_id {
        -1 => None,
        id if id >= 0 => Some(id),
        _ => return Err(DescribeClusterProtocolFailure::ControllerId),
    };
    let mut brokers = response
        .brokers
        .iter()
        .map(|broker| {
            let port = u16::try_from(broker.port)
                .ok()
                .filter(|port| *port != 0)
                .ok_or(DescribeClusterProtocolFailure::BrokerPort)?;
            Ok(ClusterBroker::new(
                broker.broker_id,
                canonical_string(broker.host.as_str()),
                port,
                broker
                    .rack
                    .as_ref()
                    .map(|rack| canonical_string(rack.as_str())),
                broker.is_fenced,
            ))
        })
        .collect::<Result<Vec<_>, DescribeClusterProtocolFailure>>()?;
    brokers.sort_unstable_by_key(ClusterBroker::id);
    Ok(DescribeClusterInput::BrokerResponded {
        description: ClusterDescription::new_with_authorized_operations(
            canonical_string(response.cluster_id.as_str()),
            controller_id,
            brokers,
            (response.cluster_authorized_operations != i32::MIN)
                .then_some(response.cluster_authorized_operations),
        ),
    })
}

fn validate_success(
    response: &DescribeClusterResponse,
    retained_bytes: usize,
    include_fenced_brokers: bool,
    include_authorized_operations: bool,
) -> Result<(), DescribeClusterProtocolFailure> {
    if response.endpoint_type != BROKER_ENDPOINT_TYPE {
        return Err(DescribeClusterProtocolFailure::EndpointType);
    }
    if response.brokers.len() > MAX_BROKERS {
        return Err(DescribeClusterProtocolFailure::BrokerCapacity);
    }
    if response.cluster_id.len() > MAX_CLUSTER_ID_BYTES {
        return Err(DescribeClusterProtocolFailure::ClusterIdBytes);
    }
    if !include_authorized_operations && response.cluster_authorized_operations != i32::MIN {
        return Err(DescribeClusterProtocolFailure::AuthorizedOperations);
    }
    let mut text_bytes = response.cluster_id.len();
    for (index, broker) in response.brokers.iter().enumerate() {
        if broker.broker_id < 0 {
            return Err(DescribeClusterProtocolFailure::BrokerId);
        }
        if response.brokers[..index]
            .iter()
            .any(|earlier| earlier.broker_id == broker.broker_id)
        {
            return Err(DescribeClusterProtocolFailure::DuplicateBrokerId);
        }
        if broker.host.is_empty() {
            return Err(DescribeClusterProtocolFailure::EmptyHost);
        }
        if broker.host.len() > MAX_HOST_BYTES {
            return Err(DescribeClusterProtocolFailure::HostBytes);
        }
        if broker.is_fenced && !include_fenced_brokers {
            return Err(DescribeClusterProtocolFailure::FencedBroker);
        }
        let rack_bytes = broker.rack.as_deref().map_or(0, str::len);
        if rack_bytes > MAX_RACK_BYTES {
            return Err(DescribeClusterProtocolFailure::RackBytes);
        }
        text_bytes = text_bytes
            .checked_add(broker.host.len())
            .and_then(|bytes| bytes.checked_add(rack_bytes))
            .ok_or(DescribeClusterProtocolFailure::RetainedBytes)?;
        let _port = u16::try_from(broker.port)
            .ok()
            .filter(|port| *port != 0)
            .ok_or(DescribeClusterProtocolFailure::BrokerPort)?;
    }
    let charge = BASE_RESULT_BYTES
        .checked_add(
            response
                .brokers
                .len()
                .checked_mul(BROKER_OWNER_BYTES)
                .ok_or(DescribeClusterProtocolFailure::RetainedBytes)?,
        )
        .and_then(|bytes| {
            text_bytes
                .checked_mul(ENGINE_TEXT_COPIES)
                .and_then(|text| bytes.checked_add(text))
        })
        .ok_or(DescribeClusterProtocolFailure::RetainedBytes)?;
    if charge > retained_bytes {
        return Err(DescribeClusterProtocolFailure::RetainedBytes);
    }
    Ok(())
}

fn bounded_diagnostic(message: Option<&str>) -> (Option<String>, bool) {
    let Some(message) = message else {
        return (None, false);
    };
    if message.len() <= MAX_DIAGNOSTIC_BYTES {
        return (Some(canonical_string(message)), false);
    }
    let mut end = MAX_DIAGNOSTIC_BYTES;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    (Some(canonical_string(&message[..end])), true)
}

fn canonical_string(value: &str) -> String {
    value.to_owned().into_boxed_str().into_string()
}
