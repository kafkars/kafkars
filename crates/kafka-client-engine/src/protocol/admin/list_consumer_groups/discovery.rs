//! Bounded broker-ID extraction from controller-routed `DescribeCluster`.

use kafka_client_core::{DescribeClusterBrokerError, DescribeClusterInput};
use kafka_wire::DescribeClusterResponse;

use crate::protocol::admin::describe_cluster::{
    DescribeClusterProtocolFailure, normalize_describe_cluster_response,
};

use super::ListConsumerGroupsProtocolFailure;

const MIN_DISCOVERY_VERSION: i16 = 0;
const MAX_DISCOVERY_VERSION: i16 = 2;
const DISCOVERY_BASE_BYTES: usize = 4096;
const BROKER_OWNER_BYTES: usize = 16;

/// Discovery result retained only until exact-broker iteration is installed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NormalizedListConsumerGroupsDiscovery {
    Brokers {
        broker_ids: Vec<i32>,
        retained_bytes: usize,
    },
    Rejected {
        error: DescribeClusterBrokerError,
        retained_bytes: usize,
    },
}

/// Validates selected protocol semantics and extracts deterministic broker IDs.
pub(crate) fn normalize_list_consumer_groups_discovery(
    selected_version: Option<i16>,
    response: &DescribeClusterResponse,
    retained_bytes: usize,
) -> Result<NormalizedListConsumerGroupsDiscovery, ListConsumerGroupsProtocolFailure> {
    let version = selected_version.ok_or(ListConsumerGroupsProtocolFailure::Compatibility)?;
    if !(MIN_DISCOVERY_VERSION..=MAX_DISCOVERY_VERSION).contains(&version) {
        return Err(ListConsumerGroupsProtocolFailure::Compatibility);
    }
    if response.throttle_time_ms < 0 {
        return Err(ListConsumerGroupsProtocolFailure::InvalidResponse);
    }
    let input = normalize_describe_cluster_response(response, retained_bytes)
        .map_err(map_discovery_failure)?;
    match input {
        DescribeClusterInput::BrokerResponded { description } => {
            let broker_ids = description
                .brokers()
                .iter()
                .map(kafka_client_core::ClusterBroker::id)
                .collect::<Vec<_>>();
            let charge = DISCOVERY_BASE_BYTES
                .checked_add(
                    broker_ids
                        .len()
                        .checked_mul(BROKER_OWNER_BYTES)
                        .ok_or(ListConsumerGroupsProtocolFailure::ResponseTooLarge)?,
                )
                .ok_or(ListConsumerGroupsProtocolFailure::ResponseTooLarge)?;
            if charge > retained_bytes {
                return Err(ListConsumerGroupsProtocolFailure::ResponseTooLarge);
            }
            Ok(NormalizedListConsumerGroupsDiscovery::Brokers {
                broker_ids,
                retained_bytes: charge,
            })
        }
        DescribeClusterInput::BrokerRejected { error } => {
            if DISCOVERY_BASE_BYTES > retained_bytes {
                return Err(ListConsumerGroupsProtocolFailure::ResponseTooLarge);
            }
            Ok(NormalizedListConsumerGroupsDiscovery::Rejected {
                error,
                retained_bytes: DISCOVERY_BASE_BYTES,
            })
        }
        _ => Err(ListConsumerGroupsProtocolFailure::InvalidResponse),
    }
}

const fn map_discovery_failure(
    failure: DescribeClusterProtocolFailure,
) -> ListConsumerGroupsProtocolFailure {
    match failure {
        DescribeClusterProtocolFailure::RetainedBytes => {
            ListConsumerGroupsProtocolFailure::ResponseTooLarge
        }
        _ => ListConsumerGroupsProtocolFailure::InvalidResponse,
    }
}
