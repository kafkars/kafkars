//! Strict name-based v4 `TxnOffsetCommit` response correlation.
//!
//! Version 4 matches the explicit `AddOffsets` step, retains topic names, and
//! carries every group-aware transactional offset error needed by this seam.
//! The normalizer accepts response reordering but never positional ambiguity.

use kafka_wire::{
    TxnOffsetCommitResponse,
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use kafka_wire_core::Uuid;

use super::{
    TransactionBrokerError, TransactionOffsetCommitRef, broker_error::transaction_broker_error,
    txn_offset_commit_request::unique_topic_count,
};

/// Exact partition-level result for one requested next offset.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionOffsetCommitOutcome {
    Committed,
    Rejected(TransactionBrokerError),
}

/// One generated response fact restored to caller offset order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TransactionOffsetCommitResultRef<'a> {
    offset: TransactionOffsetCommitRef<'a>,
    outcome: TransactionOffsetCommitOutcome,
}

#[cfg(test)]
impl<'a> TransactionOffsetCommitResultRef<'a> {
    pub(crate) const fn offset(self) -> TransactionOffsetCommitRef<'a> {
        self.offset
    }

    pub(crate) const fn outcome(self) -> TransactionOffsetCommitOutcome {
        self.outcome
    }
}

#[cfg(not(test))]
impl TransactionOffsetCommitResultRef<'_> {
    pub(crate) const fn outcome(self) -> TransactionOffsetCommitOutcome {
        self.outcome
    }
}

/// A structurally exact v4 result retaining caller order and signed errors.
#[must_use = "validated transaction offset results must be interpreted"]
pub(crate) struct ValidatedTxnOffsetCommitResponse<'a> {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "throttle validation is retained now and exposed to exact-correlation tests"
        )
    )]
    throttle_time_ms: u32,
    offsets: Vec<TransactionOffsetCommitResultRef<'a>>,
}

impl<'a> ValidatedTxnOffsetCommitResponse<'a> {
    #[cfg(test)]
    pub(crate) const fn throttle_time_ms(&self) -> u32 {
        self.throttle_time_ms
    }

    pub(crate) fn offsets(&self) -> &[TransactionOffsetCommitResultRef<'a>] {
        &self.offsets
    }
}

/// Generated response facts unsafe to bind to the requested offsets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TxnOffsetCommitResponseFailure {
    NegativeThrottleTime { actual: i32 },
    EmptyExpectedOffsets,
    EmptyExpectedTopic,
    NegativeExpectedPartition { actual: i32 },
    DuplicateExpectedOffset { actual: i32 },
    TopicCount { expected: usize, actual: usize },
    EmptyTopic,
    UnexpectedTopicId,
    DuplicateTopic,
    EmptyTopicPartitions,
    PartitionCount { expected: usize, actual: usize },
    NegativePartition { actual: i32 },
    DuplicatePartition { actual: i32 },
    MissingTopic,
    MissingPartition { actual: i32 },
    RetainedBytes,
}

/// Validates a v4 response and restores exact caller offset order.
pub(crate) fn normalize_txn_offset_commit_v4_response<'a>(
    offsets: &[TransactionOffsetCommitRef<'a>],
    response: &TxnOffsetCommitResponse,
) -> Result<ValidatedTxnOffsetCommitResponse<'a>, TxnOffsetCommitResponseFailure> {
    let throttle_time_ms = u32::try_from(response.throttle_time_ms).map_err(|_| {
        TxnOffsetCommitResponseFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        }
    })?;
    validate_expected(offsets)?;
    validate_response_shape(offsets, &response.topics)?;
    let mut results = Vec::new();
    results
        .try_reserve_exact(offsets.len())
        .map_err(|_| TxnOffsetCommitResponseFailure::RetainedBytes)?;
    for offset in offsets {
        let topic = matching_topic(offset.topic(), &response.topics)?;
        let partition = matching_partition(offset.partition(), &topic.partitions)?;
        let outcome = match transaction_broker_error(partition.error_code) {
            Some(error) => TransactionOffsetCommitOutcome::Rejected(error),
            None => TransactionOffsetCommitOutcome::Committed,
        };
        results.push(TransactionOffsetCommitResultRef {
            offset: *offset,
            outcome,
        });
    }
    Ok(ValidatedTxnOffsetCommitResponse {
        throttle_time_ms,
        offsets: results,
    })
}

fn validate_expected(
    offsets: &[TransactionOffsetCommitRef<'_>],
) -> Result<(), TxnOffsetCommitResponseFailure> {
    if offsets.is_empty() {
        return Err(TxnOffsetCommitResponseFailure::EmptyExpectedOffsets);
    }
    for (index, offset) in offsets.iter().enumerate() {
        if offset.topic().is_empty() {
            return Err(TxnOffsetCommitResponseFailure::EmptyExpectedTopic);
        }
        if offset.partition() < 0 {
            return Err(TxnOffsetCommitResponseFailure::NegativeExpectedPartition {
                actual: offset.partition(),
            });
        }
        if offsets[..index].iter().any(|previous| {
            previous.topic() == offset.topic() && previous.partition() == offset.partition()
        }) {
            return Err(TxnOffsetCommitResponseFailure::DuplicateExpectedOffset {
                actual: offset.partition(),
            });
        }
    }
    Ok(())
}

fn validate_response_shape(
    expected: &[TransactionOffsetCommitRef<'_>],
    topics: &[TxnOffsetCommitResponseTopic],
) -> Result<(), TxnOffsetCommitResponseFailure> {
    let expected_topics = unique_topic_count(expected);
    if topics.len() != expected_topics {
        return Err(TxnOffsetCommitResponseFailure::TopicCount {
            expected: expected_topics,
            actual: topics.len(),
        });
    }
    let mut partition_count = 0usize;
    for (topic_index, topic) in topics.iter().enumerate() {
        if topic.name.is_empty() {
            return Err(TxnOffsetCommitResponseFailure::EmptyTopic);
        }
        if topic.topic_id != Uuid::ZERO {
            return Err(TxnOffsetCommitResponseFailure::UnexpectedTopicId);
        }
        if topics[..topic_index]
            .iter()
            .any(|previous| previous.name == topic.name)
        {
            return Err(TxnOffsetCommitResponseFailure::DuplicateTopic);
        }
        if topic.partitions.is_empty() {
            return Err(TxnOffsetCommitResponseFailure::EmptyTopicPartitions);
        }
        validate_partitions(&topic.partitions)?;
        partition_count = partition_count
            .checked_add(topic.partitions.len())
            .ok_or(TxnOffsetCommitResponseFailure::RetainedBytes)?;
    }
    if partition_count != expected.len() {
        return Err(TxnOffsetCommitResponseFailure::PartitionCount {
            expected: expected.len(),
            actual: partition_count,
        });
    }
    Ok(())
}

fn validate_partitions(
    partitions: &[TxnOffsetCommitResponsePartition],
) -> Result<(), TxnOffsetCommitResponseFailure> {
    for (index, partition) in partitions.iter().enumerate() {
        if partition.partition_index < 0 {
            return Err(TxnOffsetCommitResponseFailure::NegativePartition {
                actual: partition.partition_index,
            });
        }
        if partitions[..index]
            .iter()
            .any(|previous| previous.partition_index == partition.partition_index)
        {
            return Err(TxnOffsetCommitResponseFailure::DuplicatePartition {
                actual: partition.partition_index,
            });
        }
    }
    Ok(())
}

fn matching_topic<'a>(
    expected: &str,
    topics: &'a [TxnOffsetCommitResponseTopic],
) -> Result<&'a TxnOffsetCommitResponseTopic, TxnOffsetCommitResponseFailure> {
    topics
        .iter()
        .find(|topic| topic.name.as_str() == expected)
        .ok_or(TxnOffsetCommitResponseFailure::MissingTopic)
}

fn matching_partition(
    expected: i32,
    partitions: &[TxnOffsetCommitResponsePartition],
) -> Result<&TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseFailure> {
    partitions
        .iter()
        .find(|partition| partition.partition_index == expected)
        .ok_or(TxnOffsetCommitResponseFailure::MissingPartition { actual: expected })
}
