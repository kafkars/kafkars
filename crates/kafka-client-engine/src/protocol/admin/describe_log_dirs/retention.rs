//! Checked request, validation-scratch, and normalized-result byte accounting.

use core::mem::size_of;

use kafka_client_core::{
    AdminDescribeLogDirsBrokerOutcome, AdminLogDirDescription, AdminLogDirOutcome,
    AdminLogDirReplicaInfo, DescribeReplicaLogDirsReplicaPlacement, ReplicaLogDirInfo,
};
use kafka_wire::{DescribeLogDirsResponse, describe_log_dirs_request::DescribableLogDirTopic};

use super::{
    DescribeLogDirsSelectionRef, DescribeLogDirsTopicSelectionRef, NormalizedDescribeLogDir,
    NormalizedDescribeLogDirsPartition, NormalizedDescribeLogDirsResponse,
    NormalizedDescribeLogDirsTopic,
};

pub(super) const MAX_TOPIC_NAME_BYTES: usize = 249;
pub(super) const MAX_LOG_DIR_PATH_BYTES: usize = i16::MAX as usize;
pub(super) const MAX_LOG_DIRS: usize = 1_024;
pub(super) const MAX_TOPICS: usize = 16 * 1_024;
pub(super) const MAX_PARTITIONS: usize = 1_024 * 1_024;

pub(super) fn request_peak_charge(
    topics: &[DescribeLogDirsTopicSelectionRef<'_>],
) -> Option<usize> {
    let topic_scratch = topics.len().checked_mul(size_of::<&str>())?;
    let mut partition_count = 0usize;
    let mut charge = topics
        .len()
        .checked_mul(size_of::<DescribableLogDirTopic>())?
        .checked_add(topic_scratch)?;
    for topic in topics {
        partition_count = partition_count.checked_add(topic.partitions().len())?;
        charge = charge
            .checked_add(topic.topic().len())?
            .checked_add(topic.partitions().len().checked_mul(size_of::<i32>())?)?;
    }
    charge.checked_add(partition_count.checked_mul(size_of::<(&str, i32)>())?)
}

pub(super) fn response_peak_charge(
    selection: DescribeLogDirsSelectionRef<'_>,
    response: &DescribeLogDirsResponse,
) -> Option<usize> {
    let mut output = size_of::<NormalizedDescribeLogDirsResponse>().checked_add(
        response
            .results
            .len()
            .checked_mul(size_of::<NormalizedDescribeLogDir>())?,
    )?;
    let mut duplicate_keys = response.results.len();
    for log_dir in &response.results {
        output = output.checked_add(log_dir.log_dir.len())?.checked_add(
            log_dir
                .topics
                .len()
                .checked_mul(size_of::<NormalizedDescribeLogDirsTopic>())?,
        )?;
        duplicate_keys = duplicate_keys.checked_add(log_dir.topics.len())?;
        for topic in &log_dir.topics {
            output = output.checked_add(topic.name.len())?.checked_add(
                topic
                    .partitions
                    .len()
                    .checked_mul(size_of::<NormalizedDescribeLogDirsPartition>())?,
            )?;
            duplicate_keys = duplicate_keys.checked_add(topic.partitions.len())?;
        }
    }
    let (selected_partitions, selected_terminal) = match selection {
        DescribeLogDirsSelectionRef::AllTopics => (0, 0),
        DescribeLogDirsSelectionRef::Selected(topics) => {
            let selected_partitions = topics.iter().try_fold(0usize, |count, topic| {
                count.checked_add(topic.partitions().len())
            })?;
            let selected_topic_bytes = topics.iter().try_fold(0usize, |bytes, topic| {
                bytes.checked_add(topic.topic().len().checked_mul(topic.partitions().len())?)
            })?;
            let repeated_path_bytes =
                response.results.iter().try_fold(0usize, |bytes, log_dir| {
                    let replicas = log_dir.topics.iter().try_fold(0usize, |count, topic| {
                        count.checked_add(topic.partitions.len())
                    })?;
                    bytes.checked_add(log_dir.log_dir.len().checked_mul(replicas)?)
                })?;
            let selected_terminal = selected_partitions
                .checked_mul(size_of::<DescribeReplicaLogDirsReplicaPlacement>())?
                .checked_add(selected_partitions.checked_mul(size_of::<ReplicaLogDirInfo>())?)?
                .checked_add(selected_topic_bytes)?
                .checked_add(repeated_path_bytes)?;
            (selected_partitions, selected_terminal)
        }
    };
    let terminal = response.results.iter().try_fold(
        size_of::<AdminDescribeLogDirsBrokerOutcome>(),
        |charge, log_dir| {
            let replica_count = log_dir.topics.iter().try_fold(0usize, |count, topic| {
                count.checked_add(topic.partitions.len())
            })?;
            let replica_topic_bytes = log_dir.topics.iter().try_fold(0usize, |bytes, topic| {
                bytes.checked_add(topic.name.len().checked_mul(topic.partitions.len())?)
            })?;
            charge
                .checked_add(size_of::<AdminLogDirOutcome>())?
                .checked_add(log_dir.log_dir.len())?
                .checked_add(size_of::<AdminLogDirDescription>())?
                .checked_add(replica_count.checked_mul(size_of::<AdminLogDirReplicaInfo>())?)?
                .checked_add(replica_topic_bytes)
        },
    )?;
    output
        .checked_add(duplicate_keys.checked_mul(size_of::<DuplicateKey<'static>>())?)?
        .checked_add(selected_partitions.checked_mul(size_of::<SelectionKey<'static>>())?)
        .and_then(|charge| charge.checked_add(selected_terminal))
        .and_then(|charge| charge.checked_add(terminal))
}

pub(super) fn normalized_retained_charge(
    response: &NormalizedDescribeLogDirsResponse,
) -> Option<usize> {
    response.log_dirs.iter().try_fold(
        size_of::<NormalizedDescribeLogDirsResponse>().checked_add(
            response
                .log_dirs
                .capacity()
                .checked_mul(size_of::<NormalizedDescribeLogDir>())?,
        )?,
        |charge, log_dir| {
            log_dir.topics.iter().try_fold(
                charge.checked_add(log_dir.path.capacity())?.checked_add(
                    log_dir
                        .topics
                        .capacity()
                        .checked_mul(size_of::<NormalizedDescribeLogDirsTopic>())?,
                )?,
                |charge, topic| {
                    charge.checked_add(topic.name.capacity())?.checked_add(
                        topic
                            .partitions
                            .capacity()
                            .checked_mul(size_of::<NormalizedDescribeLogDirsPartition>())?,
                    )
                },
            )
        },
    )
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum DuplicateKey<'a> {
    LogDir(&'a str),
    Topic(&'a str, &'a str),
    Partition(&'a str, &'a str, i32),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct SelectionKey<'a>(pub(super) &'a str, pub(super) i32);
