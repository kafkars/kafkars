//! Fallible bounded construction of generated name-based log-directory queries.

use kafka_wire::{
    DescribeLogDirsRequest, RetainedSize, describe_log_dirs_request::DescribableLogDirTopic,
};

use super::{
    DescribeLogDirsSelectionRef, DescribeLogDirsTopicSelectionRef,
    retention::{MAX_PARTITIONS, MAX_TOPIC_NAME_BYTES, MAX_TOPICS, request_peak_charge},
};

/// Invalid selection or insufficient retained capacity before driver ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeLogDirsRequestFailure {
    EmptySelection,
    TooManyTopics { actual: usize, max: usize },
    EmptyTopic,
    TopicNameTooLong { actual: usize, max: usize },
    DuplicateTopic,
    EmptyPartitions,
    TooManyPartitions { actual: usize, max: usize },
    NegativePartition { actual: i32 },
    DuplicatePartition { actual: i32 },
    RetainedBytes { required: usize, limit: usize },
}

/// Builds one API-key 35 request without owning broker routing or selection policy.
pub(crate) fn describe_log_dirs_request(
    selection: DescribeLogDirsSelectionRef<'_>,
    retained_limit: usize,
) -> Result<DescribeLogDirsRequest, DescribeLogDirsRequestFailure> {
    let DescribeLogDirsSelectionRef::Selected(topics) = selection else {
        let mut request = DescribeLogDirsRequest::default();
        request.topics = None;
        return Ok(request);
    };
    validate_selected_shape(topics)?;
    let required = request_peak_charge(topics).unwrap_or(usize::MAX);
    ensure_limit(required, retained_limit)?;
    validate_uniqueness(topics, required, retained_limit)?;

    let mut generated_topics = Vec::new();
    generated_topics
        .try_reserve_exact(topics.len())
        .map_err(|_| retained_failure(required, retained_limit))?;
    for selected in topics {
        let mut partitions = Vec::new();
        partitions
            .try_reserve_exact(selected.partitions().len())
            .map_err(|_| retained_failure(required, retained_limit))?;
        partitions.extend_from_slice(selected.partitions());
        let mut topic = DescribableLogDirTopic::default();
        topic.topic = selected.topic().into();
        topic.partitions = partitions;
        generated_topics.push(topic);
    }
    let mut request = DescribeLogDirsRequest::default();
    request.topics = Some(generated_topics);
    let actual = request.retained_size().heap_bytes();
    ensure_limit(actual, retained_limit)?;
    Ok(request)
}

fn validate_selected_shape(
    topics: &[DescribeLogDirsTopicSelectionRef<'_>],
) -> Result<(), DescribeLogDirsRequestFailure> {
    if topics.is_empty() {
        return Err(DescribeLogDirsRequestFailure::EmptySelection);
    }
    if topics.len() > MAX_TOPICS {
        return Err(DescribeLogDirsRequestFailure::TooManyTopics {
            actual: topics.len(),
            max: MAX_TOPICS,
        });
    }
    let mut partition_count = 0usize;
    for topic in topics {
        if topic.topic().is_empty() {
            return Err(DescribeLogDirsRequestFailure::EmptyTopic);
        }
        if topic.topic().len() > MAX_TOPIC_NAME_BYTES {
            return Err(DescribeLogDirsRequestFailure::TopicNameTooLong {
                actual: topic.topic().len(),
                max: MAX_TOPIC_NAME_BYTES,
            });
        }
        if topic.partitions().is_empty() {
            return Err(DescribeLogDirsRequestFailure::EmptyPartitions);
        }
        partition_count = partition_count
            .checked_add(topic.partitions().len())
            .ok_or(DescribeLogDirsRequestFailure::TooManyPartitions {
                actual: usize::MAX,
                max: MAX_PARTITIONS,
            })?;
        if partition_count > MAX_PARTITIONS {
            return Err(DescribeLogDirsRequestFailure::TooManyPartitions {
                actual: partition_count,
                max: MAX_PARTITIONS,
            });
        }
        if let Some(actual) = topic.partitions().iter().copied().find(|value| *value < 0) {
            return Err(DescribeLogDirsRequestFailure::NegativePartition { actual });
        }
    }
    Ok(())
}

fn validate_uniqueness(
    topics: &[DescribeLogDirsTopicSelectionRef<'_>],
    required: usize,
    retained_limit: usize,
) -> Result<(), DescribeLogDirsRequestFailure> {
    let mut topic_names = Vec::new();
    topic_names
        .try_reserve_exact(topics.len())
        .map_err(|_| retained_failure(required, retained_limit))?;
    topic_names.extend(topics.iter().map(|topic| topic.topic()));
    topic_names.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    if topic_names.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(DescribeLogDirsRequestFailure::DuplicateTopic);
    }

    let partition_count = topics.iter().map(|topic| topic.partitions().len()).sum();
    let mut partitions = Vec::new();
    partitions
        .try_reserve_exact(partition_count)
        .map_err(|_| retained_failure(required, retained_limit))?;
    for topic in topics {
        partitions.extend(
            topic
                .partitions()
                .iter()
                .copied()
                .map(|partition| (topic.topic(), partition)),
        );
    }
    partitions.sort_unstable_by(|left, right| left.0.cmp(right.0).then(left.1.cmp(&right.1)));
    if let Some(pair) = partitions.windows(2).find(|pair| pair[0] == pair[1]) {
        return Err(DescribeLogDirsRequestFailure::DuplicatePartition { actual: pair[0].1 });
    }
    Ok(())
}

fn ensure_limit(required: usize, limit: usize) -> Result<(), DescribeLogDirsRequestFailure> {
    (required <= limit)
        .then_some(())
        .ok_or(DescribeLogDirsRequestFailure::RetainedBytes { required, limit })
}

const fn retained_failure(required: usize, limit: usize) -> DescribeLogDirsRequestFailure {
    DescribeLogDirsRequestFailure::RetainedBytes { required, limit }
}
