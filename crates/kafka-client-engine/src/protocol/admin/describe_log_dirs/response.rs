//! Strict version-aware normalization of generated broker log-directory facts.

use kafka_wire::{
    DescribeLogDirsResponse,
    describe_log_dirs_response::{
        DescribeLogDirsPartition, DescribeLogDirsResult, DescribeLogDirsTopic,
    },
};

use super::{
    DescribeLogDirsSelectionRef, NormalizedDescribeLogDir, NormalizedDescribeLogDirsPartition,
    NormalizedDescribeLogDirsResponse, NormalizedDescribeLogDirsTopic,
    retention::{
        DuplicateKey, MAX_LOG_DIR_PATH_BYTES, MAX_LOG_DIRS, MAX_PARTITIONS, MAX_TOPIC_NAME_BYTES,
        MAX_TOPICS, SelectionKey, normalized_retained_charge, response_peak_charge,
    },
    version::supports_describe_log_dirs_version,
};

/// Generated response facts unsafe to bind to one selected broker query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeLogDirsResponseFailure {
    UnsupportedApiVersion { actual: i16 },
    NegativeThrottleTime { actual: i32 },
    UnrepresentableTopLevelError { actual: i16 },
    UnrepresentableVolumeBytes,
    UnrepresentableCordonState,
    TooManyLogDirs { actual: usize, max: usize },
    EmptyLogDir,
    LogDirPathTooLong { actual: usize, max: usize },
    DuplicateLogDir,
    TooManyTopics { actual: usize, max: usize },
    EmptyTopic,
    TopicNameTooLong { actual: usize, max: usize },
    EmptyTopicPartitions,
    DuplicateTopic,
    TooManyPartitions { actual: usize, max: usize },
    NegativePartition { actual: i32 },
    DuplicatePartition { actual: i32 },
    UnexpectedPartition { actual: i32 },
    InvalidSelection,
    RetainedBytes { required: usize, limit: usize },
}

/// Validates and owns one response using the driver-selected API version.
pub(crate) fn normalize_describe_log_dirs_response(
    selection: DescribeLogDirsSelectionRef<'_>,
    selected_version: i16,
    response: &DescribeLogDirsResponse,
    retained_limit: usize,
) -> Result<NormalizedDescribeLogDirsResponse, DescribeLogDirsResponseFailure> {
    validate_scalar_version(selected_version, response)?;
    validate_hard_shape(response)?;
    let required = response_peak_charge(selection, response).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;
    let selected = selected_keys(selection, required, retained_limit)?;
    validate_uniqueness_and_selection(response, &selected, required, retained_limit)?;
    let mut normalized = materialize(selected_version, response, required, retained_limit)?;
    let retained = normalized_retained_charge(&normalized).unwrap_or(usize::MAX);
    ensure_limit(retained, retained_limit)?;
    normalized.retained_bytes = required;
    Ok(normalized)
}

fn validate_scalar_version(
    selected_version: i16,
    response: &DescribeLogDirsResponse,
) -> Result<(), DescribeLogDirsResponseFailure> {
    if !supports_describe_log_dirs_version(selected_version) {
        return Err(DescribeLogDirsResponseFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    u32::try_from(response.throttle_time_ms).map_err(|_| {
        DescribeLogDirsResponseFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    if selected_version < 3 && response.error_code != 0 {
        return Err(
            DescribeLogDirsResponseFailure::UnrepresentableTopLevelError {
                actual: response.error_code,
            },
        );
    }
    for log_dir in &response.results {
        if selected_version < 4 && (log_dir.total_bytes != -1 || log_dir.usable_bytes != -1) {
            return Err(DescribeLogDirsResponseFailure::UnrepresentableVolumeBytes);
        }
        if selected_version < 5 && log_dir.is_cordoned {
            return Err(DescribeLogDirsResponseFailure::UnrepresentableCordonState);
        }
    }
    Ok(())
}

fn validate_hard_shape(
    response: &DescribeLogDirsResponse,
) -> Result<(), DescribeLogDirsResponseFailure> {
    if response.results.len() > MAX_LOG_DIRS {
        return Err(DescribeLogDirsResponseFailure::TooManyLogDirs {
            actual: response.results.len(),
            max: MAX_LOG_DIRS,
        });
    }
    let mut topics = 0usize;
    let mut partitions = 0usize;
    for log_dir in &response.results {
        validate_log_dir_path(log_dir)?;
        topics = topics
            .checked_add(log_dir.topics.len())
            .unwrap_or(usize::MAX);
        if topics > MAX_TOPICS {
            return Err(DescribeLogDirsResponseFailure::TooManyTopics {
                actual: topics,
                max: MAX_TOPICS,
            });
        }
        for topic in &log_dir.topics {
            validate_topic(topic)?;
            partitions = partitions
                .checked_add(topic.partitions.len())
                .unwrap_or(usize::MAX);
            if partitions > MAX_PARTITIONS {
                return Err(DescribeLogDirsResponseFailure::TooManyPartitions {
                    actual: partitions,
                    max: MAX_PARTITIONS,
                });
            }
            if let Some(actual) = topic
                .partitions
                .iter()
                .map(|partition| partition.partition_index)
                .find(|partition| *partition < 0)
            {
                return Err(DescribeLogDirsResponseFailure::NegativePartition { actual });
            }
        }
    }
    Ok(())
}

fn validate_log_dir_path(
    log_dir: &DescribeLogDirsResult,
) -> Result<(), DescribeLogDirsResponseFailure> {
    if log_dir.log_dir.is_empty() {
        return Err(DescribeLogDirsResponseFailure::EmptyLogDir);
    }
    if log_dir.log_dir.len() > MAX_LOG_DIR_PATH_BYTES {
        return Err(DescribeLogDirsResponseFailure::LogDirPathTooLong {
            actual: log_dir.log_dir.len(),
            max: MAX_LOG_DIR_PATH_BYTES,
        });
    }
    Ok(())
}

fn validate_topic(topic: &DescribeLogDirsTopic) -> Result<(), DescribeLogDirsResponseFailure> {
    if topic.name.is_empty() {
        return Err(DescribeLogDirsResponseFailure::EmptyTopic);
    }
    if topic.name.len() > MAX_TOPIC_NAME_BYTES {
        return Err(DescribeLogDirsResponseFailure::TopicNameTooLong {
            actual: topic.name.len(),
            max: MAX_TOPIC_NAME_BYTES,
        });
    }
    if topic.partitions.is_empty() {
        return Err(DescribeLogDirsResponseFailure::EmptyTopicPartitions);
    }
    Ok(())
}

fn selected_keys<'a>(
    selection: DescribeLogDirsSelectionRef<'a>,
    required: usize,
    limit: usize,
) -> Result<Option<Vec<SelectionKey<'a>>>, DescribeLogDirsResponseFailure> {
    let DescribeLogDirsSelectionRef::Selected(topics) = selection else {
        return Ok(None);
    };
    let count = topics.iter().try_fold(0usize, |count, topic| {
        count.checked_add(topic.partitions().len())
    });
    let Some(count) = count else {
        return Err(DescribeLogDirsResponseFailure::InvalidSelection);
    };
    let mut keys = Vec::new();
    keys.try_reserve_exact(count)
        .map_err(|_| retained_failure(required, limit))?;
    for topic in topics {
        if topic.topic().is_empty()
            || topic.topic().len() > MAX_TOPIC_NAME_BYTES
            || topic.partitions().is_empty()
        {
            return Err(DescribeLogDirsResponseFailure::InvalidSelection);
        }
        for partition in topic.partitions() {
            if *partition < 0 {
                return Err(DescribeLogDirsResponseFailure::InvalidSelection);
            }
            keys.push(SelectionKey(topic.topic(), *partition));
        }
    }
    keys.sort_unstable();
    if keys.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(DescribeLogDirsResponseFailure::InvalidSelection);
    }
    Ok(Some(keys))
}

fn validate_uniqueness_and_selection(
    response: &DescribeLogDirsResponse,
    selected: &Option<Vec<SelectionKey<'_>>>,
    required: usize,
    limit: usize,
) -> Result<(), DescribeLogDirsResponseFailure> {
    let count = response.results.iter().try_fold(0usize, |count, log_dir| {
        log_dir
            .topics
            .iter()
            .try_fold(count.checked_add(1)?, |count, topic| {
                count.checked_add(1)?.checked_add(topic.partitions.len())
            })
    });
    let Some(count) = count else {
        return Err(retained_failure(required, limit));
    };
    let mut keys = Vec::new();
    keys.try_reserve_exact(count)
        .map_err(|_| retained_failure(required, limit))?;
    for log_dir in &response.results {
        let path = log_dir.log_dir.as_str();
        keys.push(DuplicateKey::LogDir(path));
        for topic in &log_dir.topics {
            let name = topic.name.as_str();
            keys.push(DuplicateKey::Topic(path, name));
            for partition in &topic.partitions {
                if selected.as_ref().is_some_and(|selected| {
                    selected
                        .binary_search(&SelectionKey(name, partition.partition_index))
                        .is_err()
                }) {
                    return Err(DescribeLogDirsResponseFailure::UnexpectedPartition {
                        actual: partition.partition_index,
                    });
                }
                keys.push(DuplicateKey::Partition(
                    path,
                    name,
                    partition.partition_index,
                ));
            }
        }
    }
    keys.sort_unstable();
    for pair in keys.windows(2) {
        if pair[0] != pair[1] {
            continue;
        }
        return Err(match pair[0] {
            DuplicateKey::LogDir(_) => DescribeLogDirsResponseFailure::DuplicateLogDir,
            DuplicateKey::Topic(_, _) => DescribeLogDirsResponseFailure::DuplicateTopic,
            DuplicateKey::Partition(_, _, actual) => {
                DescribeLogDirsResponseFailure::DuplicatePartition { actual }
            }
        });
    }
    Ok(())
}

fn materialize(
    selected_version: i16,
    response: &DescribeLogDirsResponse,
    required: usize,
    limit: usize,
) -> Result<NormalizedDescribeLogDirsResponse, DescribeLogDirsResponseFailure> {
    let mut log_dirs = Vec::new();
    log_dirs
        .try_reserve_exact(response.results.len())
        .map_err(|_| retained_failure(required, limit))?;
    for generated in &response.results {
        log_dirs.push(materialize_log_dir(
            selected_version,
            generated,
            required,
            limit,
        )?);
    }
    log_dirs.sort_unstable_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
    Ok(NormalizedDescribeLogDirsResponse {
        selected_version,
        throttle_time_ms: response.throttle_time_ms as u32,
        error_code: response.error_code,
        log_dirs,
        retained_bytes: 0,
    })
}

fn materialize_log_dir(
    selected_version: i16,
    generated: &DescribeLogDirsResult,
    required: usize,
    limit: usize,
) -> Result<NormalizedDescribeLogDir, DescribeLogDirsResponseFailure> {
    let mut topics = Vec::new();
    topics
        .try_reserve_exact(generated.topics.len())
        .map_err(|_| retained_failure(required, limit))?;
    for topic in &generated.topics {
        topics.push(materialize_topic(topic, required, limit)?);
    }
    topics.sort_unstable_by(|left, right| left.name.as_bytes().cmp(right.name.as_bytes()));
    Ok(NormalizedDescribeLogDir {
        error_code: generated.error_code,
        path: copy_string(generated.log_dir.as_str(), required, limit)?,
        topics,
        total_bytes: (selected_version >= 4).then_some(generated.total_bytes),
        usable_bytes: (selected_version >= 4).then_some(generated.usable_bytes),
        is_cordoned: (selected_version >= 5).then_some(generated.is_cordoned),
    })
}

fn materialize_topic(
    generated: &DescribeLogDirsTopic,
    required: usize,
    limit: usize,
) -> Result<NormalizedDescribeLogDirsTopic, DescribeLogDirsResponseFailure> {
    let mut partitions = Vec::new();
    partitions
        .try_reserve_exact(generated.partitions.len())
        .map_err(|_| retained_failure(required, limit))?;
    partitions.extend(generated.partitions.iter().map(normalize_partition));
    partitions.sort_unstable_by_key(|partition| partition.partition_index);
    Ok(NormalizedDescribeLogDirsTopic {
        name: copy_string(generated.name.as_str(), required, limit)?,
        partitions,
    })
}

const fn normalize_partition(
    generated: &DescribeLogDirsPartition,
) -> NormalizedDescribeLogDirsPartition {
    NormalizedDescribeLogDirsPartition {
        partition_index: generated.partition_index,
        partition_size: generated.partition_size,
        offset_lag: generated.offset_lag,
        is_future: generated.is_future_key,
    }
}

fn copy_string(
    source: &str,
    required: usize,
    limit: usize,
) -> Result<String, DescribeLogDirsResponseFailure> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(source.len())
        .map_err(|_| retained_failure(required, limit))?;
    owned.push_str(source);
    Ok(owned)
}

fn ensure_limit(required: usize, limit: usize) -> Result<(), DescribeLogDirsResponseFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(DescribeLogDirsResponseFailure::RetainedBytes { required, limit })
}

const fn retained_failure(required: usize, limit: usize) -> DescribeLogDirsResponseFailure {
    DescribeLogDirsResponseFailure::RetainedBytes { required, limit }
}
