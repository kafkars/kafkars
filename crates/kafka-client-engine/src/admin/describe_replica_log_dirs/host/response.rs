//! Exhaustive raw-driver, selected-response, and replica-placement translation.

use core::{mem::size_of, num::NonZeroI16};

use kafka_client_core::{
    DeliveryStatus, DescribeReplicaLogDirsBrokerError, DescribeReplicaLogDirsInput,
    DescribeReplicaLogDirsReplica, DescribeReplicaLogDirsReplicaPlacement, ReplicaLogDirInfo,
    ReplicaLogDirLocation,
};

use crate::{
    driver::{
        DescribeReplicaLogDirsDriverFailureKind, DescribeReplicaLogDirsRawTerminal,
        DescribeReplicaLogDirsTerminalFact,
    },
    protocol::admin::describe_log_dirs::{
        DescribeLogDirsResponseFailure, DescribeLogDirsSelectionRef,
        DescribeLogDirsTopicSelectionRef, NormalizedDescribeLogDir,
        NormalizedDescribeLogDirsResponse, normalize_describe_log_dirs_response,
    },
};

pub(super) fn terminal_input(
    raw: &DescribeReplicaLogDirsRawTerminal,
    current_broker: i32,
    replicas: &[DescribeReplicaLogDirsReplica],
    retained_bytes: usize,
) -> (DescribeReplicaLogDirsInput, usize) {
    if !replicas
        .iter()
        .all(|replica| replica.broker_id() == current_broker)
    {
        return (DescribeReplicaLogDirsInput::InvalidResponse, 0);
    }
    match raw.fact() {
        DescribeReplicaLogDirsTerminalFact::Response {
            broker_id,
            selected_version: Some(selected_version),
            response,
        } if broker_id == current_broker => {
            let Ok(groups) = selection_groups(replicas, retained_bytes) else {
                return (DescribeReplicaLogDirsInput::ResponseTooLarge, 0);
            };
            let mut selection_refs = Vec::new();
            if selection_refs.try_reserve_exact(groups.len()).is_err() {
                return (DescribeReplicaLogDirsInput::ResponseTooLarge, 0);
            }
            for group in &groups {
                selection_refs.push(DescribeLogDirsTopicSelectionRef::new(
                    group.topic,
                    &group.partitions,
                ));
            }
            let Some(scratch_bytes) =
                selection_scratch_bytes(&groups, groups.capacity(), selection_refs.capacity())
            else {
                return (DescribeReplicaLogDirsInput::ResponseTooLarge, 0);
            };
            let Some(normalized_limit) = retained_bytes.checked_sub(scratch_bytes) else {
                return (DescribeReplicaLogDirsInput::ResponseTooLarge, 0);
            };
            match normalize_describe_log_dirs_response(
                DescribeLogDirsSelectionRef::Selected(&selection_refs),
                selected_version,
                response,
                normalized_limit,
            ) {
                Ok(normalized) => match normalized_input(broker_id, replicas, normalized) {
                    Ok(result) => result,
                    Err(CorrelationFailure::RetainedBytes) => {
                        (DescribeReplicaLogDirsInput::ResponseTooLarge, 0)
                    }
                    Err(CorrelationFailure::Invalid) => {
                        (DescribeReplicaLogDirsInput::InvalidResponse, 0)
                    }
                },
                Err(DescribeLogDirsResponseFailure::RetainedBytes { .. }) => {
                    (DescribeReplicaLogDirsInput::ResponseTooLarge, 0)
                }
                Err(DescribeLogDirsResponseFailure::UnsupportedApiVersion { .. }) => (
                    DescribeReplicaLogDirsInput::ProtocolIncompatible {
                        delivery: DeliveryStatus::PossiblySent,
                    },
                    0,
                ),
                Err(_) => (DescribeReplicaLogDirsInput::InvalidResponse, 0),
            }
        }
        DescribeReplicaLogDirsTerminalFact::Response {
            selected_version: None,
            ..
        } => (
            DescribeReplicaLogDirsInput::ProtocolIncompatible {
                delivery: DeliveryStatus::PossiblySent,
            },
            0,
        ),
        DescribeReplicaLogDirsTerminalFact::Response { .. } => {
            (DescribeReplicaLogDirsInput::InvalidResponse, 0)
        }
        DescribeReplicaLogDirsTerminalFact::Failed { kind, delivery } => {
            (driver_failure(kind, delivery), 0)
        }
    }
}

struct SelectionGroup<'a> {
    topic: &'a str,
    partitions: Vec<i32>,
}

fn selection_groups(
    replicas: &[DescribeReplicaLogDirsReplica],
    retained_limit: usize,
) -> Result<Vec<SelectionGroup<'_>>, ()> {
    let per_replica_bytes = size_of::<SelectionGroup<'_>>()
        .checked_add(size_of::<i32>())
        .and_then(|bytes| bytes.checked_add(size_of::<DescribeLogDirsTopicSelectionRef<'_>>()))
        .ok_or(())?;
    let worst_case_bytes = replicas.len().checked_mul(per_replica_bytes).ok_or(())?;
    retained_limit.checked_sub(worst_case_bytes).ok_or(())?;
    let mut groups: Vec<SelectionGroup<'_>> = Vec::new();
    groups.try_reserve_exact(replicas.len()).map_err(|_| ())?;
    for replica in replicas {
        if let Some(group) = groups
            .iter_mut()
            .find(|group| group.topic == replica.topic())
        {
            group.partitions.try_reserve(1).map_err(|_| ())?;
            group.partitions.push(replica.partition());
        } else {
            let mut partitions = Vec::new();
            partitions.try_reserve_exact(1).map_err(|_| ())?;
            partitions.push(replica.partition());
            groups.push(SelectionGroup {
                topic: replica.topic(),
                partitions,
            });
        }
    }
    groups.shrink_to_fit();
    let scratch_bytes =
        selection_scratch_bytes(&groups, groups.capacity(), groups.len()).ok_or(())?;
    if scratch_bytes > retained_limit {
        return Err(());
    }
    Ok(groups)
}

fn selection_scratch_bytes(
    groups: &[SelectionGroup<'_>],
    group_capacity: usize,
    selection_capacity: usize,
) -> Option<usize> {
    group_capacity
        .checked_mul(size_of::<SelectionGroup<'_>>())
        .and_then(|bytes| {
            groups.iter().try_fold(bytes, |bytes, group| {
                bytes.checked_add(group.partitions.capacity().checked_mul(size_of::<i32>())?)
            })
        })
        .and_then(|bytes| {
            bytes.checked_add(
                selection_capacity.checked_mul(size_of::<DescribeLogDirsTopicSelectionRef<'_>>())?,
            )
        })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CorrelationFailure {
    RetainedBytes,
    Invalid,
}

pub(super) fn normalized_input(
    broker_id: i32,
    replicas: &[DescribeReplicaLogDirsReplica],
    normalized: NormalizedDescribeLogDirsResponse,
) -> Result<(DescribeReplicaLogDirsInput, usize), CorrelationFailure> {
    let (throttle_time_ms, error_code, log_dirs, retained_bytes) = normalized.into_parts();
    let result = match NonZeroI16::new(error_code) {
        Some(code) => Err(DescribeReplicaLogDirsBrokerError::new(code)),
        None => Ok(correlate(replicas, log_dirs)?),
    };
    Ok((
        DescribeReplicaLogDirsInput::BrokerResponded {
            broker_id,
            throttle_time_ms,
            result,
        },
        retained_bytes,
    ))
}

fn correlate(
    replicas: &[DescribeReplicaLogDirsReplica],
    log_dirs: Vec<NormalizedDescribeLogDir>,
) -> Result<Vec<DescribeReplicaLogDirsReplicaPlacement>, CorrelationFailure> {
    let mut placements = Vec::new();
    placements
        .try_reserve_exact(replicas.len())
        .map_err(|_| CorrelationFailure::RetainedBytes)?;
    placements.extend((0..replicas.len()).map(|_| ReplicaLogDirInfo::new(None, None)));

    for log_dir in log_dirs {
        let (error_code, path, topics, _, _, _) = log_dir.into_parts();
        if error_code != 0 {
            continue;
        }
        for topic in topics {
            let (topic, partitions) = topic.into_parts();
            for partition in partitions {
                let (partition, _, offset_lag, future) = partition.into_parts();
                let index = replicas
                    .iter()
                    .position(|replica| {
                        replica.topic() == topic && replica.partition() == partition
                    })
                    .ok_or(CorrelationFailure::Invalid)?;
                let (current, next) =
                    core::mem::replace(&mut placements[index], ReplicaLogDirInfo::new(None, None))
                        .into_parts();
                let location = ReplicaLogDirLocation::new(copy_string(&path)?, offset_lag);
                let updated = if future {
                    if next.is_some() {
                        return Err(CorrelationFailure::Invalid);
                    }
                    ReplicaLogDirInfo::new(current, Some(location))
                } else {
                    if current.is_some() {
                        return Err(CorrelationFailure::Invalid);
                    }
                    ReplicaLogDirInfo::new(Some(location), next)
                };
                placements[index] = updated;
            }
        }
    }

    let mut correlated = Vec::new();
    correlated
        .try_reserve_exact(replicas.len())
        .map_err(|_| CorrelationFailure::RetainedBytes)?;
    for (replica, info) in replicas.iter().cloned().zip(placements) {
        correlated.push(DescribeReplicaLogDirsReplicaPlacement::new(replica, info));
    }
    Ok(correlated)
}

fn copy_string(source: &str) -> Result<String, CorrelationFailure> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(source.len())
        .map_err(|_| CorrelationFailure::RetainedBytes)?;
    owned.push_str(source);
    Ok(owned)
}

const fn driver_failure(
    kind: DescribeReplicaLogDirsDriverFailureKind,
    delivery: DeliveryStatus,
) -> DescribeReplicaLogDirsInput {
    match kind {
        DescribeReplicaLogDirsDriverFailureKind::DeadlineElapsed => {
            DescribeReplicaLogDirsInput::DriverDeadlineElapsed { delivery }
        }
        DescribeReplicaLogDirsDriverFailureKind::Compatibility => {
            DescribeReplicaLogDirsInput::ProtocolIncompatible { delivery }
        }
        DescribeReplicaLogDirsDriverFailureKind::InvalidResponse => {
            DescribeReplicaLogDirsInput::InvalidResponse
        }
        DescribeReplicaLogDirsDriverFailureKind::Transport => {
            DescribeReplicaLogDirsInput::TransportFailed { delivery }
        }
    }
}
