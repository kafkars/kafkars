//! Fallible preparation of one explicit assigned-partition `OffsetFetch`.

use std::sync::Arc;

use super::{
    model::{GroupOffsetFetchCorrelation, GroupOffsetFetchTopic},
    request::{
        GroupOffsetFetchRequest, GroupOffsetFetchRequestBuildFailure,
        try_group_offset_fetch_request,
    },
};

/// Local preparation failures before any driver request exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetFetchRequestPreparationFailure {
    EmptyGroup,
    EmptyTopic,
    EmptyTopicPartitions,
    DuplicateTopic,
    NegativePartition { actual: i32 },
    DuplicatePartition { actual: i32 },
    PartitionCount,
    Allocation,
    RetainedBytes { required: usize, limit: usize },
}

/// Explicit local empty-assignment or request-bearing preparation.
#[must_use = "empty assignments and prepared requests require distinct handling"]
#[expect(
    clippy::large_enum_variant,
    reason = "boxing would add an untracked allocation to fallible request preparation"
)]
pub(crate) enum GroupOffsetFetchPreparation {
    /// No assigned partition exists, so no Kafka request may be submitted.
    NoRequest,
    /// One explicit assigned-partition request and independent correlation.
    Prepared(PreparedGroupOffsetFetch),
}

/// Correlation and driver input prepared together, then separated linearly.
#[must_use = "prepared request ownership must be submitted or deliberately released"]
pub(crate) struct PreparedGroupOffsetFetch {
    correlation: GroupOffsetFetchCorrelation,
    request: PreparedGroupOffsetFetchRequest,
}

impl PreparedGroupOffsetFetch {
    pub(crate) fn into_parts(
        self,
    ) -> (GroupOffsetFetchCorrelation, PreparedGroupOffsetFetchRequest) {
        (self.correlation, self.request)
    }
}

/// Linear protocol request owner handed to the future driver adapter.
#[must_use = "a prepared group offset fetch request must be submitted or released"]
pub(crate) struct PreparedGroupOffsetFetchRequest {
    request: GroupOffsetFetchRequest,
    retained_bytes: usize,
}

impl PreparedGroupOffsetFetchRequest {
    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Transfers the independently version-selecting wire request wrapper.
    pub(crate) fn into_wire_request(self) -> GroupOffsetFetchRequest {
        self.request
    }
}

/// Validates and prepares one explicit assignment without execution policy.
pub(crate) fn prepare_group_offset_fetch_request(
    group_id: Arc<str>,
    topics: Vec<GroupOffsetFetchTopic>,
    request_byte_limit: usize,
) -> Result<GroupOffsetFetchPreparation, GroupOffsetFetchRequestPreparationFailure> {
    if group_id.is_empty() {
        return Err(GroupOffsetFetchRequestPreparationFailure::EmptyGroup);
    }
    if topics.is_empty() {
        return Ok(GroupOffsetFetchPreparation::NoRequest);
    }
    let partition_count = validate_topics(&topics)?;
    let request =
        try_group_offset_fetch_request(group_id.as_ref(), &topics).map_err(map_build_failure)?;
    let retained_bytes = request.retained_bytes();
    if retained_bytes > request_byte_limit {
        return Err(GroupOffsetFetchRequestPreparationFailure::RetainedBytes {
            required: retained_bytes,
            limit: request_byte_limit,
        });
    }
    let request = PreparedGroupOffsetFetchRequest {
        request,
        retained_bytes,
    };
    Ok(GroupOffsetFetchPreparation::Prepared(
        PreparedGroupOffsetFetch {
            correlation: GroupOffsetFetchCorrelation::new(group_id, topics, partition_count),
            request,
        },
    ))
}

const fn map_build_failure(
    failure: GroupOffsetFetchRequestBuildFailure,
) -> GroupOffsetFetchRequestPreparationFailure {
    match failure {
        GroupOffsetFetchRequestBuildFailure::Allocation => {
            GroupOffsetFetchRequestPreparationFailure::Allocation
        }
    }
}

fn validate_topics(
    topics: &[GroupOffsetFetchTopic],
) -> Result<usize, GroupOffsetFetchRequestPreparationFailure> {
    let mut partition_count = 0usize;
    for (topic_index, topic) in topics.iter().enumerate() {
        if topic.name().is_empty() {
            return Err(GroupOffsetFetchRequestPreparationFailure::EmptyTopic);
        }
        if topic.partition_indexes().is_empty() {
            return Err(GroupOffsetFetchRequestPreparationFailure::EmptyTopicPartitions);
        }
        if topics[..topic_index]
            .iter()
            .any(|previous| previous.name() == topic.name())
        {
            return Err(GroupOffsetFetchRequestPreparationFailure::DuplicateTopic);
        }
        partition_count = partition_count
            .checked_add(topic.partition_indexes().len())
            .ok_or(GroupOffsetFetchRequestPreparationFailure::PartitionCount)?;
        for (partition_index, partition) in topic.partition_indexes().iter().copied().enumerate() {
            if partition < 0 {
                return Err(
                    GroupOffsetFetchRequestPreparationFailure::NegativePartition {
                        actual: partition,
                    },
                );
            }
            if topic.partition_indexes()[..partition_index].contains(&partition) {
                return Err(
                    GroupOffsetFetchRequestPreparationFailure::DuplicatePartition {
                        actual: partition,
                    },
                );
            }
        }
    }
    Ok(partition_count)
}
