//! Strict caller-order correlation of generated API-key 34 partition results.

use kafka_wire::AlterReplicaLogDirsResponse;

use super::{
    AlterReplicaLogDirAssignmentRef, NormalizedAlterReplicaLogDirOutcome,
    NormalizedAlterReplicaLogDirsResponse,
    retention::{
        Expected, MAX_ASSIGNMENTS, MAX_LOG_DIR_PATH_BYTES, MAX_TOPIC_GROUPS, MAX_TOPIC_NAME_BYTES,
        Returned, actual_response_peak_charge, response_peak_charge,
    },
    version::supports_alter_replica_log_dirs_version,
};

/// Generated response facts unsafe to bind to the destructive assignment batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterReplicaLogDirsResponseFailure {
    UnsupportedApiVersion { actual: i16 },
    NegativeThrottleTime { actual: i32 },
    InvalidAssignment,
    TooManyTopics { actual: usize, max: usize },
    EmptyTopic,
    TopicNameTooLong { actual: usize, max: usize },
    EmptyTopicPartitions,
    TooManyPartitions { actual: usize, max: usize },
    NegativePartition { actual: i32 },
    TopicCount,
    PartitionCount,
    DuplicateTopic,
    DuplicatePartition { actual: i32 },
    UnexpectedTopic,
    MissingTopic,
    UnexpectedPartition { actual: i32 },
    MissingPartition { actual: i32 },
    RetainedBytes { required: usize, limit: usize },
}

/// Validates one selected-version response before owning caller-ordered facts.
pub(crate) fn normalize_alter_replica_log_dirs_response(
    assignments: &[AlterReplicaLogDirAssignmentRef<'_>],
    selected_version: i16,
    response: &AlterReplicaLogDirsResponse,
    retained_limit: usize,
) -> Result<NormalizedAlterReplicaLogDirsResponse, AlterReplicaLogDirsResponseFailure> {
    if !supports_alter_replica_log_dirs_version(selected_version) {
        return Err(AlterReplicaLogDirsResponseFailure::UnsupportedApiVersion {
            actual: selected_version,
        });
    }
    validate_assignment_shape(assignments)?;
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        AlterReplicaLogDirsResponseFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    let returned_count = validate_response_shape(response)?;
    let required = response_peak_charge(assignments, returned_count).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;
    let expected = expected(assignments, required, retained_limit)?;
    let returned = returned(response, returned_count, required, retained_limit)?;
    correlate(&expected, &returned, response.results.len())?;
    let outcomes = normalize_in_caller_order(assignments, &returned, required, retained_limit)?;
    let mut normalized = NormalizedAlterReplicaLogDirsResponse {
        selected_version,
        throttle_time_ms,
        outcomes,
        retained_bytes: required,
    };
    let actual = actual_response_peak_charge(&normalized, expected.capacity(), returned.capacity())
        .unwrap_or(usize::MAX);
    ensure_limit(actual, retained_limit)?;
    normalized.retained_bytes = required.max(actual);
    Ok(normalized)
}

fn validate_assignment_shape(
    assignments: &[AlterReplicaLogDirAssignmentRef<'_>],
) -> Result<(), AlterReplicaLogDirsResponseFailure> {
    if assignments.is_empty() || assignments.len() > MAX_ASSIGNMENTS {
        return Err(AlterReplicaLogDirsResponseFailure::InvalidAssignment);
    }
    if assignments.iter().any(|assignment| {
        assignment.topic().is_empty()
            || assignment.topic().len() > MAX_TOPIC_NAME_BYTES
            || assignment.log_dir().is_empty()
            || assignment.log_dir().len() > MAX_LOG_DIR_PATH_BYTES
            || assignment.partition() < 0
    }) {
        return Err(AlterReplicaLogDirsResponseFailure::InvalidAssignment);
    }
    Ok(())
}

fn validate_response_shape(
    response: &AlterReplicaLogDirsResponse,
) -> Result<usize, AlterReplicaLogDirsResponseFailure> {
    if response.results.len() > MAX_TOPIC_GROUPS {
        return Err(AlterReplicaLogDirsResponseFailure::TooManyTopics {
            actual: response.results.len(),
            max: MAX_TOPIC_GROUPS,
        });
    }
    let mut partition_count = 0usize;
    for topic in &response.results {
        if topic.topic_name.is_empty() {
            return Err(AlterReplicaLogDirsResponseFailure::EmptyTopic);
        }
        if topic.topic_name.len() > MAX_TOPIC_NAME_BYTES {
            return Err(AlterReplicaLogDirsResponseFailure::TopicNameTooLong {
                actual: topic.topic_name.len(),
                max: MAX_TOPIC_NAME_BYTES,
            });
        }
        if topic.partitions.is_empty() {
            return Err(AlterReplicaLogDirsResponseFailure::EmptyTopicPartitions);
        }
        partition_count = partition_count
            .checked_add(topic.partitions.len())
            .unwrap_or(usize::MAX);
        if partition_count > MAX_ASSIGNMENTS {
            return Err(AlterReplicaLogDirsResponseFailure::TooManyPartitions {
                actual: partition_count,
                max: MAX_ASSIGNMENTS,
            });
        }
        if let Some(actual) = topic
            .partitions
            .iter()
            .map(|partition| partition.partition_index)
            .find(|partition| *partition < 0)
        {
            return Err(AlterReplicaLogDirsResponseFailure::NegativePartition { actual });
        }
    }
    Ok(partition_count)
}

fn expected<'a>(
    assignments: &[AlterReplicaLogDirAssignmentRef<'a>],
    required: usize,
    limit: usize,
) -> Result<Vec<Expected<'a>>, AlterReplicaLogDirsResponseFailure> {
    let mut expected = Vec::new();
    expected
        .try_reserve_exact(assignments.len())
        .map_err(|_| retained_failure(required, limit))?;
    for (caller_index, assignment) in assignments.iter().copied().enumerate() {
        expected.push(Expected {
            topic: assignment.topic(),
            partition: assignment.partition(),
            caller_index,
        });
    }
    expected.sort_unstable_by(expected_order);
    if let Some(pair) = expected.windows(2).find(|pair| {
        same_identity(
            pair[0].topic,
            pair[0].partition,
            pair[1].topic,
            pair[1].partition,
        )
    }) {
        return Err(AlterReplicaLogDirsResponseFailure::DuplicatePartition {
            actual: pair[0].partition,
        });
    }
    Ok(expected)
}

fn returned(
    response: &AlterReplicaLogDirsResponse,
    partition_count: usize,
    required: usize,
    limit: usize,
) -> Result<Vec<Returned<'_>>, AlterReplicaLogDirsResponseFailure> {
    let mut returned = Vec::new();
    returned
        .try_reserve_exact(partition_count)
        .map_err(|_| retained_failure(required, limit))?;
    for (source_topic, topic) in response.results.iter().enumerate() {
        returned.extend(topic.partitions.iter().map(|partition| Returned {
            topic: topic.topic_name.as_str(),
            partition: partition.partition_index,
            error_code: partition.error_code,
            source_topic,
        }));
    }
    returned.sort_unstable_by(returned_order);
    for pair in returned.windows(2) {
        if pair[0].topic == pair[1].topic && pair[0].source_topic != pair[1].source_topic {
            return Err(AlterReplicaLogDirsResponseFailure::DuplicateTopic);
        }
        if same_identity(
            pair[0].topic,
            pair[0].partition,
            pair[1].topic,
            pair[1].partition,
        ) {
            return Err(AlterReplicaLogDirsResponseFailure::DuplicatePartition {
                actual: pair[0].partition,
            });
        }
    }
    Ok(returned)
}

fn correlate(
    expected: &[Expected<'_>],
    returned: &[Returned<'_>],
    returned_topic_count: usize,
) -> Result<(), AlterReplicaLogDirsResponseFailure> {
    if unique_topic_count(expected) != returned_topic_count {
        return Err(AlterReplicaLogDirsResponseFailure::TopicCount);
    }
    if expected.len() != returned.len() {
        return Err(AlterReplicaLogDirsResponseFailure::PartitionCount);
    }
    for (expected, returned) in expected.iter().zip(returned) {
        match returned.topic.as_bytes().cmp(expected.topic.as_bytes()) {
            core::cmp::Ordering::Less => {
                return Err(AlterReplicaLogDirsResponseFailure::UnexpectedTopic);
            }
            core::cmp::Ordering::Greater => {
                return Err(AlterReplicaLogDirsResponseFailure::MissingTopic);
            }
            core::cmp::Ordering::Equal => {}
        }
        match returned.partition.cmp(&expected.partition) {
            core::cmp::Ordering::Less => {
                return Err(AlterReplicaLogDirsResponseFailure::UnexpectedPartition {
                    actual: returned.partition,
                });
            }
            core::cmp::Ordering::Greater => {
                return Err(AlterReplicaLogDirsResponseFailure::MissingPartition {
                    actual: expected.partition,
                });
            }
            core::cmp::Ordering::Equal => {}
        }
    }
    Ok(())
}

fn normalize_in_caller_order(
    assignments: &[AlterReplicaLogDirAssignmentRef<'_>],
    returned: &[Returned<'_>],
    required: usize,
    limit: usize,
) -> Result<Vec<NormalizedAlterReplicaLogDirOutcome>, AlterReplicaLogDirsResponseFailure> {
    let mut outcomes = Vec::new();
    outcomes
        .try_reserve_exact(assignments.len())
        .map_err(|_| retained_failure(required, limit))?;
    for assignment in assignments {
        let index = returned
            .binary_search_by(|returned| {
                returned
                    .topic
                    .as_bytes()
                    .cmp(assignment.topic().as_bytes())
                    .then_with(|| returned.partition.cmp(&assignment.partition()))
            })
            .map_err(|_| AlterReplicaLogDirsResponseFailure::MissingPartition {
                actual: assignment.partition(),
            })?;
        outcomes.push(NormalizedAlterReplicaLogDirOutcome {
            topic: copy_string(assignment.topic(), required, limit)?,
            partition: assignment.partition(),
            error_code: returned[index].error_code,
        });
    }
    Ok(outcomes)
}

fn expected_order(left: &Expected<'_>, right: &Expected<'_>) -> core::cmp::Ordering {
    left.topic
        .as_bytes()
        .cmp(right.topic.as_bytes())
        .then_with(|| left.partition.cmp(&right.partition))
        .then_with(|| left.caller_index.cmp(&right.caller_index))
}

fn returned_order(left: &Returned<'_>, right: &Returned<'_>) -> core::cmp::Ordering {
    left.topic
        .as_bytes()
        .cmp(right.topic.as_bytes())
        .then_with(|| left.partition.cmp(&right.partition))
        .then_with(|| left.source_topic.cmp(&right.source_topic))
}

fn unique_topic_count(expected: &[Expected<'_>]) -> usize {
    expected
        .iter()
        .enumerate()
        .filter(|(index, entry)| *index == 0 || expected[*index - 1].topic != entry.topic)
        .count()
}

fn same_identity(
    left_topic: &str,
    left_partition: i32,
    right_topic: &str,
    right_partition: i32,
) -> bool {
    left_topic == right_topic && left_partition == right_partition
}

fn copy_string(
    source: &str,
    required: usize,
    limit: usize,
) -> Result<String, AlterReplicaLogDirsResponseFailure> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(source.len())
        .map_err(|_| retained_failure(required, limit))?;
    owned.push_str(source);
    Ok(owned)
}

fn ensure_limit(required: usize, limit: usize) -> Result<(), AlterReplicaLogDirsResponseFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(AlterReplicaLogDirsResponseFailure::RetainedBytes { required, limit })
}

const fn retained_failure(required: usize, limit: usize) -> AlterReplicaLogDirsResponseFailure {
    AlterReplicaLogDirsResponseFailure::RetainedBytes { required, limit }
}
