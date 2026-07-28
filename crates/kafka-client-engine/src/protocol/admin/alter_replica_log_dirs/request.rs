//! Checked and fallible grouping into generated API-key 34 request ownership.

use kafka_wire::{
    AlterReplicaLogDirsRequest,
    alter_replica_log_dirs_request::{AlterReplicaLogDir, AlterReplicaLogDirTopic},
};

use super::{
    AlterReplicaLogDirAssignmentRef,
    retention::{
        MAX_ASSIGNMENTS, MAX_LOG_DIR_PATH_BYTES, MAX_LOG_DIRS, MAX_TOPIC_GROUPS,
        MAX_TOPIC_NAME_BYTES, actual_request_peak_charge, request_peak_charge,
    },
};

/// Invalid assignment shape or insufficient bytes before driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlterReplicaLogDirsRequestFailure {
    EmptyAssignments,
    TooManyAssignments { actual: usize, max: usize },
    EmptyTopic,
    TopicNameTooLong { actual: usize, max: usize },
    EmptyLogDir,
    LogDirPathTooLong { actual: usize, max: usize },
    NegativePartition { actual: i32 },
    DuplicateReplica { partition: i32 },
    TooManyLogDirs { actual: usize, max: usize },
    TooManyTopicGroups { actual: usize, max: usize },
    RetainedBytes { required: usize, limit: usize },
}

/// Builds one generated request for assignments already grouped to one broker.
pub(crate) fn alter_replica_log_dirs_request(
    assignments: &[AlterReplicaLogDirAssignmentRef<'_>],
    retained_limit: usize,
) -> Result<AlterReplicaLogDirsRequest, AlterReplicaLogDirsRequestFailure> {
    validate_scalar_shape(assignments)?;
    let required = request_peak_charge(assignments).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;
    let mut order = caller_indices(assignments, required, retained_limit)?;
    validate_unique_replicas(assignments, &mut order)?;
    order.sort_unstable_by(|left, right| grouped_order(assignments, *left, *right));
    let (log_dir_count, topic_group_count) = grouped_counts(assignments, &order);
    validate_group_counts(log_dir_count, topic_group_count)?;
    let request = materialize(assignments, &order, log_dir_count, required, retained_limit)?;
    let actual = actual_request_peak_charge(&request, order.capacity()).unwrap_or(usize::MAX);
    ensure_limit(actual, retained_limit)?;
    Ok(request)
}

fn validate_scalar_shape(
    assignments: &[AlterReplicaLogDirAssignmentRef<'_>],
) -> Result<(), AlterReplicaLogDirsRequestFailure> {
    if assignments.is_empty() {
        return Err(AlterReplicaLogDirsRequestFailure::EmptyAssignments);
    }
    if assignments.len() > MAX_ASSIGNMENTS {
        return Err(AlterReplicaLogDirsRequestFailure::TooManyAssignments {
            actual: assignments.len(),
            max: MAX_ASSIGNMENTS,
        });
    }
    for assignment in assignments {
        if assignment.topic().is_empty() {
            return Err(AlterReplicaLogDirsRequestFailure::EmptyTopic);
        }
        if assignment.topic().len() > MAX_TOPIC_NAME_BYTES {
            return Err(AlterReplicaLogDirsRequestFailure::TopicNameTooLong {
                actual: assignment.topic().len(),
                max: MAX_TOPIC_NAME_BYTES,
            });
        }
        if assignment.log_dir().is_empty() {
            return Err(AlterReplicaLogDirsRequestFailure::EmptyLogDir);
        }
        if assignment.log_dir().len() > MAX_LOG_DIR_PATH_BYTES {
            return Err(AlterReplicaLogDirsRequestFailure::LogDirPathTooLong {
                actual: assignment.log_dir().len(),
                max: MAX_LOG_DIR_PATH_BYTES,
            });
        }
        if assignment.partition() < 0 {
            return Err(AlterReplicaLogDirsRequestFailure::NegativePartition {
                actual: assignment.partition(),
            });
        }
    }
    Ok(())
}

fn caller_indices(
    assignments: &[AlterReplicaLogDirAssignmentRef<'_>],
    required: usize,
    limit: usize,
) -> Result<Vec<usize>, AlterReplicaLogDirsRequestFailure> {
    let mut order = Vec::new();
    order
        .try_reserve_exact(assignments.len())
        .map_err(|_| retained_failure(required, limit))?;
    order.extend(0..assignments.len());
    Ok(order)
}

fn validate_unique_replicas(
    assignments: &[AlterReplicaLogDirAssignmentRef<'_>],
    order: &mut [usize],
) -> Result<(), AlterReplicaLogDirsRequestFailure> {
    order.sort_unstable_by(|left, right| {
        assignments[*left]
            .topic()
            .as_bytes()
            .cmp(assignments[*right].topic().as_bytes())
            .then_with(|| {
                assignments[*left]
                    .partition()
                    .cmp(&assignments[*right].partition())
            })
            .then_with(|| left.cmp(right))
    });
    for pair in order.windows(2) {
        let left = assignments[pair[0]];
        let right = assignments[pair[1]];
        if left.topic() == right.topic() && left.partition() == right.partition() {
            return Err(AlterReplicaLogDirsRequestFailure::DuplicateReplica {
                partition: left.partition(),
            });
        }
    }
    Ok(())
}

fn grouped_order(
    assignments: &[AlterReplicaLogDirAssignmentRef<'_>],
    left: usize,
    right: usize,
) -> core::cmp::Ordering {
    assignments[left]
        .log_dir()
        .as_bytes()
        .cmp(assignments[right].log_dir().as_bytes())
        .then_with(|| {
            assignments[left]
                .topic()
                .as_bytes()
                .cmp(assignments[right].topic().as_bytes())
        })
        .then_with(|| {
            assignments[left]
                .partition()
                .cmp(&assignments[right].partition())
        })
        .then_with(|| left.cmp(&right))
}

fn grouped_counts(
    assignments: &[AlterReplicaLogDirAssignmentRef<'_>],
    order: &[usize],
) -> (usize, usize) {
    let mut log_dirs = 0usize;
    let mut topic_groups = 0usize;
    for (position, index) in order.iter().copied().enumerate() {
        let assignment = assignments[index];
        if position == 0 || assignments[order[position - 1]].log_dir() != assignment.log_dir() {
            log_dirs += 1;
        }
        if position == 0
            || assignments[order[position - 1]].log_dir() != assignment.log_dir()
            || assignments[order[position - 1]].topic() != assignment.topic()
        {
            topic_groups += 1;
        }
    }
    (log_dirs, topic_groups)
}

fn validate_group_counts(
    log_dirs: usize,
    topic_groups: usize,
) -> Result<(), AlterReplicaLogDirsRequestFailure> {
    if log_dirs > MAX_LOG_DIRS {
        return Err(AlterReplicaLogDirsRequestFailure::TooManyLogDirs {
            actual: log_dirs,
            max: MAX_LOG_DIRS,
        });
    }
    if topic_groups > MAX_TOPIC_GROUPS {
        return Err(AlterReplicaLogDirsRequestFailure::TooManyTopicGroups {
            actual: topic_groups,
            max: MAX_TOPIC_GROUPS,
        });
    }
    Ok(())
}

fn materialize(
    assignments: &[AlterReplicaLogDirAssignmentRef<'_>],
    order: &[usize],
    log_dir_count: usize,
    required: usize,
    limit: usize,
) -> Result<AlterReplicaLogDirsRequest, AlterReplicaLogDirsRequestFailure> {
    let mut dirs = Vec::new();
    dirs.try_reserve_exact(log_dir_count)
        .map_err(|_| retained_failure(required, limit))?;
    let mut cursor = 0usize;
    while cursor < order.len() {
        let log_dir = assignments[order[cursor]].log_dir();
        let log_dir_end = run_end(assignments, order, cursor, |assignment| {
            assignment.log_dir() == log_dir
        });
        dirs.push(materialize_log_dir(
            assignments,
            &order[cursor..log_dir_end],
            required,
            limit,
        )?);
        cursor = log_dir_end;
    }
    let mut request = AlterReplicaLogDirsRequest::default();
    request.dirs = dirs;
    Ok(request)
}

fn materialize_log_dir(
    assignments: &[AlterReplicaLogDirAssignmentRef<'_>],
    order: &[usize],
    required: usize,
    limit: usize,
) -> Result<AlterReplicaLogDir, AlterReplicaLogDirsRequestFailure> {
    let topic_count = 1 + order
        .windows(2)
        .filter(|pair| assignments[pair[0]].topic() != assignments[pair[1]].topic())
        .count();
    let mut topics = Vec::new();
    topics
        .try_reserve_exact(topic_count)
        .map_err(|_| retained_failure(required, limit))?;
    let mut cursor = 0usize;
    while cursor < order.len() {
        let topic = assignments[order[cursor]].topic();
        let topic_end = run_end(assignments, order, cursor, |assignment| {
            assignment.topic() == topic
        });
        let mut partitions = Vec::new();
        partitions
            .try_reserve_exact(topic_end - cursor)
            .map_err(|_| retained_failure(required, limit))?;
        partitions.extend(
            order[cursor..topic_end]
                .iter()
                .map(|index| assignments[*index].partition()),
        );
        let mut generated = AlterReplicaLogDirTopic::default();
        generated.name = topic.into();
        generated.partitions = partitions;
        topics.push(generated);
        cursor = topic_end;
    }
    let mut generated = AlterReplicaLogDir::default();
    generated.path = assignments[order[0]].log_dir().into();
    generated.topics = topics;
    Ok(generated)
}

fn run_end(
    assignments: &[AlterReplicaLogDirAssignmentRef<'_>],
    order: &[usize],
    start: usize,
    belongs: impl Fn(AlterReplicaLogDirAssignmentRef<'_>) -> bool,
) -> usize {
    let mut end = start + 1;
    while end < order.len() && belongs(assignments[order[end]]) {
        end += 1;
    }
    end
}

fn ensure_limit(required: usize, limit: usize) -> Result<(), AlterReplicaLogDirsRequestFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(AlterReplicaLogDirsRequestFailure::RetainedBytes { required, limit })
}

const fn retained_failure(required: usize, limit: usize) -> AlterReplicaLogDirsRequestFailure {
    AlterReplicaLogDirsRequestFailure::RetainedBytes { required, limit }
}
