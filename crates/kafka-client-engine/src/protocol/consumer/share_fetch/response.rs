//! Top-level `ShareFetch` v1 correlation and response normalization.

use core::num::NonZeroI16;

use kafka_wire::{ShareFetchResponse, share_fetch_response::NodeEndpoint};

use super::{
    SHARE_FETCH_MAX_ENDPOINTS, SHARE_FETCH_MAX_TOPICS, SHARE_FETCH_MAX_VERSION,
    SHARE_FETCH_MIN_VERSION, ShareFetchBrokerRejection, ShareFetchCorrelation, ShareFetchEndpoint,
    ShareFetchOutcome, ShareFetchResponseFailure, ShareFetchResponseLimits, ShareFetchSuccess,
    ShareFetchTopic,
    response_partition::{ShareFetchBudget, normalize_partition},
};

pub(crate) fn normalize_share_fetch_response(
    selected_version: i16,
    response: ShareFetchResponse,
    correlation: &ShareFetchCorrelation,
    limits: ShareFetchResponseLimits,
) -> Result<ShareFetchOutcome, ShareFetchResponseFailure> {
    if !(SHARE_FETCH_MIN_VERSION..=SHARE_FETCH_MAX_VERSION).contains(&selected_version) {
        return Err(ShareFetchResponseFailure::UnsupportedApiVersion(
            selected_version,
        ));
    }
    let throttle_time_ms = u32::try_from(response.throttle_time_ms)
        .map_err(|_| ShareFetchResponseFailure::NegativeThrottleTime(response.throttle_time_ms))?;
    if let Some(error_code) = NonZeroI16::new(response.error_code) {
        return Ok(ShareFetchOutcome::Rejected(ShareFetchBrokerRejection {
            throttle_time_ms,
            error_code,
        }));
    }
    let acquisition_lock_timeout_ms = normalize_lock_timeout(response.acquisition_lock_timeout_ms)?;
    if response.responses.len() > SHARE_FETCH_MAX_TOPICS {
        return Err(ShareFetchResponseFailure::TopicCount {
            actual: response.responses.len(),
            limit: SHARE_FETCH_MAX_TOPICS,
        });
    }
    if response.node_endpoints.len() > SHARE_FETCH_MAX_ENDPOINTS {
        return Err(ShareFetchResponseFailure::EndpointCount {
            actual: response.node_endpoints.len(),
            limit: SHARE_FETCH_MAX_ENDPOINTS,
        });
    }
    let mut budget = ShareFetchBudget::new(limits);
    let mut topics = Vec::new();
    topics
        .try_reserve_exact(response.responses.len())
        .map_err(|_| ShareFetchResponseFailure::Allocation)?;
    for source in response.responses {
        let topic_id = source.topic_id.to_bytes();
        if topic_id == [0; 16] {
            return Err(ShareFetchResponseFailure::ZeroTopicId);
        }
        if !correlation.contains_topic(topic_id) {
            return Err(ShareFetchResponseFailure::UnknownTopic);
        }
        if topics
            .iter()
            .any(|topic: &ShareFetchTopic| topic.topic_id == topic_id)
        {
            return Err(ShareFetchResponseFailure::DuplicateTopic);
        }
        budget.add_partitions(source.partitions.len())?;
        let mut partitions = Vec::new();
        partitions
            .try_reserve_exact(source.partitions.len())
            .map_err(|_| ShareFetchResponseFailure::Allocation)?;
        for partition in source.partitions {
            let normalized = normalize_partition(partition, topic_id, correlation, &mut budget)?;
            if partitions
                .iter()
                .any(|candidate: &super::ShareFetchPartition| {
                    candidate.partition == normalized.partition
                })
            {
                return Err(ShareFetchResponseFailure::DuplicatePartition(
                    normalized.partition,
                ));
            }
            partitions.push(normalized);
        }
        topics.push(ShareFetchTopic {
            topic_id,
            partitions,
        });
    }
    let mut endpoints = Vec::new();
    endpoints
        .try_reserve_exact(response.node_endpoints.len())
        .map_err(|_| ShareFetchResponseFailure::Allocation)?;
    for endpoint in response.node_endpoints {
        let normalized = normalize_endpoint(endpoint, &mut budget)?;
        if endpoints
            .iter()
            .any(|candidate: &ShareFetchEndpoint| candidate.node_id == normalized.node_id)
        {
            return Err(ShareFetchResponseFailure::DuplicateEndpoint(
                normalized.node_id,
            ));
        }
        endpoints.push(normalized);
    }
    Ok(ShareFetchOutcome::Succeeded(ShareFetchSuccess {
        throttle_time_ms,
        acquisition_lock_timeout_ms,
        topics,
        endpoints,
        retained_records: budget.records(),
        retained_bytes: budget.bytes(),
    }))
}

fn normalize_lock_timeout(timeout_ms: i32) -> Result<Option<u32>, ShareFetchResponseFailure> {
    match timeout_ms {
        -1 => Ok(None),
        timeout if timeout > 0 => {
            Ok(Some(u32::try_from(timeout).map_err(|_| {
                ShareFetchResponseFailure::InvalidLockTimeout(timeout)
            })?))
        }
        timeout => Err(ShareFetchResponseFailure::InvalidLockTimeout(timeout)),
    }
}

#[expect(
    clippy::redundant_closure_for_method_calls,
    reason = "kafka-wire does not publicly name the generated StrBytes field type"
)]
fn normalize_endpoint(
    endpoint: NodeEndpoint,
    budget: &mut ShareFetchBudget,
) -> Result<ShareFetchEndpoint, ShareFetchResponseFailure> {
    if endpoint.node_id < 0 {
        return Err(ShareFetchResponseFailure::InvalidEndpointNodeId(
            endpoint.node_id,
        ));
    }
    if endpoint.host.as_str().is_empty() {
        return Err(ShareFetchResponseFailure::EmptyEndpointHost);
    }
    let port = u16::try_from(endpoint.port)
        .ok()
        .filter(|port| *port != 0)
        .ok_or(ShareFetchResponseFailure::InvalidEndpointPort(
            endpoint.port,
        ))?;
    budget.add_bytes(endpoint.host.len())?;
    if let Some(rack) = endpoint.rack.as_ref() {
        budget.add_bytes(rack.len())?;
    }
    Ok(ShareFetchEndpoint {
        node_id: endpoint.node_id,
        host: endpoint.host.into_bytes(),
        port,
        rack: endpoint.rack.map(|rack| rack.into_bytes()),
    })
}
