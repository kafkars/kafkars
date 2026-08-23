//! Top-level exact correlation and bounded response normalization.

use core::num::NonZeroI16;

use kafka_wire::{ShareAcknowledgeResponse, share_acknowledge_response::PartitionData};

use super::{
    SHARE_ACKNOWLEDGE_MAX_VERSION, SHARE_ACKNOWLEDGE_MIN_VERSION, ShareAcknowledgeBrokerRejection,
    ShareAcknowledgeCorrelation, ShareAcknowledgeOutcome, ShareAcknowledgePartitionOutcome,
    ShareAcknowledgeResponseFailure, ShareAcknowledgeSuccess,
    model::{
        SHARE_ACKNOWLEDGE_MAX_ENDPOINTS, SHARE_ACKNOWLEDGE_MAX_PARTITIONS,
        SHARE_ACKNOWLEDGE_MAX_TOPICS,
    },
    response_values::{diagnostic, normalize_endpoints, normalize_leader},
};

const NOT_LEADER_OR_FOLLOWER: i16 = 6;

pub(crate) fn normalize_share_acknowledge_response(
    selected_version: i16,
    response: ShareAcknowledgeResponse,
    correlation: &ShareAcknowledgeCorrelation,
) -> Result<ShareAcknowledgeOutcome, ShareAcknowledgeResponseFailure> {
    if !(SHARE_ACKNOWLEDGE_MIN_VERSION..=SHARE_ACKNOWLEDGE_MAX_VERSION).contains(&selected_version)
    {
        return Err(ShareAcknowledgeResponseFailure::UnsupportedApiVersion(
            selected_version,
        ));
    }
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        ShareAcknowledgeResponseFailure::NegativeThrottleTime(response.throttle_time_ms)
    })?;
    if response.acquisition_lock_timeout_ms != 0 {
        return Err(ShareAcknowledgeResponseFailure::UnexpectedV2LockTimeout(
            response.acquisition_lock_timeout_ms,
        ));
    }
    if let Some(error_code) = NonZeroI16::new(response.error_code) {
        return Ok(ShareAcknowledgeOutcome::Rejected(
            ShareAcknowledgeBrokerRejection {
                throttle_time_ms,
                error_code,
                error_message: diagnostic(response.error_message)?,
            },
        ));
    }
    if response.error_message.is_some() {
        return Err(ShareAcknowledgeResponseFailure::UnexpectedErrorMessage);
    }
    validate_top_counts(&response)?;
    let endpoints = normalize_endpoints(response.node_endpoints)?;
    let mut observed = Vec::new();
    observed
        .try_reserve_exact(correlation.partitions.len())
        .map_err(|_| ShareAcknowledgeResponseFailure::Allocation)?;
    for topic in response.responses {
        let topic_id = topic.topic_id.to_bytes();
        if topic_id == [0; 16] {
            return Err(ShareAcknowledgeResponseFailure::ZeroTopicId);
        }
        if !correlation
            .partitions
            .iter()
            .any(|expected| expected.topic_id == topic_id)
        {
            return Err(ShareAcknowledgeResponseFailure::UnknownTopic);
        }
        if observed
            .iter()
            .any(|outcome: &ShareAcknowledgePartitionOutcome| outcome.topic_id == topic_id)
        {
            return Err(ShareAcknowledgeResponseFailure::DuplicateTopic);
        }
        for partition in topic.partitions {
            let outcome = normalize_partition(partition, topic_id, correlation)?;
            if observed
                .iter()
                .any(|candidate: &ShareAcknowledgePartitionOutcome| {
                    candidate.topic_id == topic_id && candidate.partition == outcome.partition
                })
            {
                return Err(ShareAcknowledgeResponseFailure::DuplicatePartition(
                    outcome.partition,
                ));
            }
            observed.push(outcome);
        }
    }
    let outcomes = restore_correlation(correlation, observed)?;
    for outcome in &outcomes {
        if let Some((leader_id, _epoch)) = outcome.current_leader
            && !endpoints
                .iter()
                .any(|endpoint| endpoint.node_id == leader_id)
        {
            return Err(ShareAcknowledgeResponseFailure::MissingLeaderEndpoint(
                leader_id,
            ));
        }
    }
    Ok(ShareAcknowledgeOutcome::Succeeded(
        ShareAcknowledgeSuccess {
            throttle_time_ms,
            outcomes,
            endpoints,
        },
    ))
}

fn validate_top_counts(
    response: &ShareAcknowledgeResponse,
) -> Result<(), ShareAcknowledgeResponseFailure> {
    if response.responses.len() > SHARE_ACKNOWLEDGE_MAX_TOPICS {
        return Err(ShareAcknowledgeResponseFailure::TopicCount {
            actual: response.responses.len(),
            limit: SHARE_ACKNOWLEDGE_MAX_TOPICS,
        });
    }
    let partitions = response
        .responses
        .iter()
        .try_fold(0usize, |count, topic| {
            count.checked_add(topic.partitions.len())
        })
        .ok_or(ShareAcknowledgeResponseFailure::PartitionCount {
            actual: usize::MAX,
            limit: SHARE_ACKNOWLEDGE_MAX_PARTITIONS,
        })?;
    if partitions > SHARE_ACKNOWLEDGE_MAX_PARTITIONS {
        return Err(ShareAcknowledgeResponseFailure::PartitionCount {
            actual: partitions,
            limit: SHARE_ACKNOWLEDGE_MAX_PARTITIONS,
        });
    }
    if response.node_endpoints.len() > SHARE_ACKNOWLEDGE_MAX_ENDPOINTS {
        return Err(ShareAcknowledgeResponseFailure::EndpointCount {
            actual: response.node_endpoints.len(),
            limit: SHARE_ACKNOWLEDGE_MAX_ENDPOINTS,
        });
    }
    Ok(())
}

fn normalize_partition(
    source: PartitionData,
    topic_id: [u8; 16],
    correlation: &ShareAcknowledgeCorrelation,
) -> Result<ShareAcknowledgePartitionOutcome, ShareAcknowledgeResponseFailure> {
    let partition = u32::try_from(source.partition_index)
        .map_err(|_| ShareAcknowledgeResponseFailure::NegativePartition(source.partition_index))?;
    if !correlation.contains(topic_id, partition) {
        return Err(ShareAcknowledgeResponseFailure::UnknownPartition(partition));
    }
    let error_code = NonZeroI16::new(source.error_code);
    if error_code.is_none() && source.error_message.is_some() {
        return Err(ShareAcknowledgeResponseFailure::UnexpectedErrorMessage);
    }
    Ok(ShareAcknowledgePartitionOutcome {
        topic_id,
        partition,
        error_code,
        error_message: diagnostic(source.error_message)?,
        current_leader: if source.error_code == NOT_LEADER_OR_FOLLOWER {
            normalize_leader(
                source.current_leader.leader_id,
                source.current_leader.leader_epoch,
            )?
        } else {
            None
        },
    })
}

fn restore_correlation(
    correlation: &ShareAcknowledgeCorrelation,
    mut observed: Vec<ShareAcknowledgePartitionOutcome>,
) -> Result<Vec<ShareAcknowledgePartitionOutcome>, ShareAcknowledgeResponseFailure> {
    if observed.len() != correlation.partitions.len() {
        return Err(ShareAcknowledgeResponseFailure::MissingPartition);
    }
    let mut outcomes = Vec::new();
    outcomes
        .try_reserve_exact(observed.len())
        .map_err(|_| ShareAcknowledgeResponseFailure::Allocation)?;
    for expected in &correlation.partitions {
        let index = observed
            .iter()
            .position(|outcome| {
                outcome.topic_id == expected.topic_id && outcome.partition == expected.partition
            })
            .ok_or(ShareAcknowledgeResponseFailure::MissingPartition)?;
        outcomes.push(observed.remove(index));
    }
    Ok(outcomes)
}
