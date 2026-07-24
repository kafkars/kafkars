//! Ordered normalization from generated Fetch DTOs and record batches.

use kafka_wire::{
    FetchResponse as WireFetchResponse,
    fetch_response::{FetchableTopicResponse, PartitionData},
};

use super::{
    batch::decode_batches,
    failure::{FetchDecodeFailure, FetchPartitionOffset},
    limits::{FetchBudget, FetchDecodeLimits},
    model::{
        FetchAbortedTransaction, FetchEndpoint, FetchEpochEndOffset, FetchLeader, FetchPartition,
        FetchResponse, FetchTopic,
    },
};

/// Normalizes one bounded generated response without retaining wire DTOs.
pub(super) fn normalize_fetch_response(
    response: WireFetchResponse,
    limits: FetchDecodeLimits,
) -> Result<FetchResponse, FetchDecodeFailure> {
    if response.throttle_time_ms < 0 {
        return Err(FetchDecodeFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        });
    }
    if response.session_id < 0 {
        return Err(FetchDecodeFailure::NegativeSessionId {
            actual: response.session_id,
        });
    }
    let mut budget = FetchBudget::start(&response, limits)?;
    let mut topics = Vec::with_capacity(response.responses.len());
    for (topic_index, topic) in response.responses.into_iter().enumerate() {
        topics.push(normalize_topic(topic, topic_index, &mut budget)?);
    }
    let endpoints = response
        .node_endpoints
        .into_iter()
        .map(normalize_endpoint)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(FetchResponse {
        throttle_time_ms: u32::try_from(response.throttle_time_ms).map_err(|_| {
            FetchDecodeFailure::NegativeThrottleTime {
                actual: response.throttle_time_ms,
            }
        })?,
        error_code: response.error_code,
        session_id: response.session_id,
        topics,
        endpoints,
    })
}

fn normalize_topic(
    topic: FetchableTopicResponse,
    topic_index: usize,
    budget: &mut FetchBudget,
) -> Result<FetchTopic, FetchDecodeFailure> {
    budget.add_partitions(topic.partitions.len())?;
    let mut partitions = Vec::with_capacity(topic.partitions.len());
    for (partition_index, partition) in topic.partitions.into_iter().enumerate() {
        partitions.push(normalize_partition(
            partition,
            topic_index,
            partition_index,
            budget,
        )?);
    }
    Ok(FetchTopic {
        name: topic.topic.into_bytes(),
        topic_id: topic.topic_id.to_bytes(),
        partitions,
    })
}

fn normalize_partition(
    partition: PartitionData,
    topic_index: usize,
    partition_index: usize,
    budget: &mut FetchBudget,
) -> Result<FetchPartition, FetchDecodeFailure> {
    let index = u32::try_from(partition.partition_index).map_err(|_| {
        FetchDecodeFailure::NegativePartitionIndex {
            actual: partition.partition_index,
        }
    })?;
    let diverging_epoch = normalize_epoch_end(
        partition.diverging_epoch.epoch,
        partition.diverging_epoch.end_offset,
    )?;
    let snapshot_id = normalize_epoch_end(
        partition.snapshot_id.epoch,
        partition.snapshot_id.end_offset,
    )?;
    let current_leader = normalize_leader(
        partition.current_leader.leader_id,
        partition.current_leader.leader_epoch,
    )?;
    let preferred_read_replica = match partition.preferred_read_replica {
        -1 => None,
        value if value >= 0 => Some(value),
        actual => return Err(FetchDecodeFailure::InvalidPreferredReplica { actual }),
    };
    let high_watermark = normalize_partition_offset(
        partition.high_watermark,
        FetchPartitionOffset::HighWatermark,
    )?;
    let last_stable_offset = normalize_partition_offset(
        partition.last_stable_offset,
        FetchPartitionOffset::LastStableOffset,
    )?;
    let log_start_offset = normalize_partition_offset(
        partition.log_start_offset,
        FetchPartitionOffset::LogStartOffset,
    )?;
    let aborted = partition.aborted_transactions.unwrap_or_default();
    budget.add_aborted_transactions(aborted.len())?;
    let aborted_transactions = aborted
        .into_iter()
        .map(|transaction| {
            if transaction.producer_id < 0 || transaction.first_offset < 0 {
                return Err(FetchDecodeFailure::InvalidAbortedTransaction {
                    producer_id: transaction.producer_id,
                    first_offset: transaction.first_offset,
                });
            }
            Ok(FetchAbortedTransaction {
                producer_id: transaction.producer_id,
                first_offset: transaction.first_offset,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let batches = decode_batches(
        partition.records.unwrap_or_default(),
        topic_index,
        partition_index,
        budget,
    )?;
    Ok(FetchPartition {
        index,
        error_code: partition.error_code,
        high_watermark,
        last_stable_offset,
        log_start_offset,
        diverging_epoch,
        current_leader,
        snapshot_id,
        preferred_read_replica,
        aborted_transactions,
        batches,
    })
}

#[expect(
    clippy::redundant_closure_for_method_calls,
    reason = "kafka-wire does not publicly name the generated field's StrBytes type"
)]
fn normalize_endpoint(
    endpoint: kafka_wire::fetch_response::NodeEndpoint,
) -> Result<FetchEndpoint, FetchDecodeFailure> {
    if endpoint.node_id < 0 {
        return Err(FetchDecodeFailure::InvalidEndpointNodeId {
            actual: endpoint.node_id,
        });
    }
    let port = u16::try_from(endpoint.port)
        .ok()
        .filter(|port| *port != 0)
        .ok_or(FetchDecodeFailure::InvalidEndpointPort {
            actual: endpoint.port,
        })?;
    Ok(FetchEndpoint {
        node_id: endpoint.node_id,
        host: endpoint.host.into_bytes(),
        port,
        rack: endpoint.rack.map(|rack| rack.into_bytes()),
    })
}

fn normalize_epoch_end(
    epoch: i32,
    end_offset: i64,
) -> Result<Option<FetchEpochEndOffset>, FetchDecodeFailure> {
    match (epoch, end_offset) {
        (-1, -1) => Ok(None),
        (epoch, end_offset) if epoch >= 0 && end_offset >= 0 => {
            Ok(Some(FetchEpochEndOffset { epoch, end_offset }))
        }
        _ => Err(FetchDecodeFailure::InvalidEpochEndOffset { epoch, end_offset }),
    }
}

fn normalize_leader(
    leader_id: i32,
    leader_epoch: i32,
) -> Result<Option<FetchLeader>, FetchDecodeFailure> {
    match (leader_id, leader_epoch) {
        (-1, -1) => Ok(None),
        (broker_id, epoch) if broker_id >= 0 && epoch >= 0 => {
            Ok(Some(FetchLeader { broker_id, epoch }))
        }
        _ => Err(FetchDecodeFailure::InvalidCurrentLeader {
            leader_id,
            leader_epoch,
        }),
    }
}

fn normalize_partition_offset(
    value: i64,
    fact: FetchPartitionOffset,
) -> Result<Option<i64>, FetchDecodeFailure> {
    match value {
        -1 => Ok(None),
        value if value >= 0 => Ok(Some(value)),
        actual => Err(FetchDecodeFailure::InvalidPartitionOffset { fact, actual }),
    }
}
