//! Exact correlation of generated v3 `AddPartitionsToTxn` results.

use kafka_wire::{
    AddPartitionsToTxnResponse,
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    },
};

use super::{
    TransactionBrokerError, TransactionPartitionRef,
    add_partitions_to_txn_request::unique_topic_count, broker_error::transaction_broker_error,
};

/// Exact partition-level result restored to caller target order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AddPartitionsToTxnPartitionOutcome {
    Added,
    Rejected(TransactionBrokerError),
}

/// One correlated generated response fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AddPartitionsToTxnPartitionResultRef<'a> {
    #[cfg(test)]
    topic: &'a str,
    #[cfg(test)]
    partition: i32,
    #[cfg(not(test))]
    _target: core::marker::PhantomData<&'a ()>,
    outcome: AddPartitionsToTxnPartitionOutcome,
}

#[cfg(test)]
impl<'a> AddPartitionsToTxnPartitionResultRef<'a> {
    pub(crate) const fn topic(self) -> &'a str {
        self.topic
    }

    pub(crate) const fn partition(self) -> i32 {
        self.partition
    }

    pub(crate) const fn outcome(self) -> AddPartitionsToTxnPartitionOutcome {
        self.outcome
    }
}

#[cfg(not(test))]
impl AddPartitionsToTxnPartitionResultRef<'_> {
    pub(crate) const fn outcome(self) -> AddPartitionsToTxnPartitionOutcome {
        self.outcome
    }
}

/// A structurally valid v3 result set with signed broker facts.
#[must_use = "validated transaction partition results must be interpreted"]
pub(crate) struct ValidatedAddPartitionsToTxnResponse<'a> {
    partitions: Vec<AddPartitionsToTxnPartitionResultRef<'a>>,
}

impl<'a> ValidatedAddPartitionsToTxnResponse<'a> {
    pub(crate) fn partitions(&self) -> &[AddPartitionsToTxnPartitionResultRef<'a>] {
        &self.partitions
    }
}

/// Generated response facts that cannot bind to the requested v3 targets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AddPartitionsToTxnResponseFailure {
    NegativeThrottleTime { actual: i32 },
    UnexpectedTopLevelError { actual: i16 },
    UnexpectedTransactionResults,
    TopicCount { expected: usize, actual: usize },
    EmptyTopic,
    EmptyTopicPartitions,
    PartitionCount { expected: usize, actual: usize },
    NegativePartition { actual: i32 },
    DuplicateTopic,
    DuplicatePartition { actual: i32 },
    MissingTopic,
    MissingPartition { actual: i32 },
    RetainedBytes,
}

/// Validates and restores one v3 result to exact caller target order.
pub(crate) fn normalize_add_partitions_to_txn_v3_response<'a>(
    targets: &[TransactionPartitionRef<'a>],
    response: &AddPartitionsToTxnResponse,
) -> Result<ValidatedAddPartitionsToTxnResponse<'a>, AddPartitionsToTxnResponseFailure> {
    if response.throttle_time_ms < 0 {
        return Err(AddPartitionsToTxnResponseFailure::NegativeThrottleTime {
            actual: response.throttle_time_ms,
        });
    }
    validate_v3_shape(targets, response)?;
    let mut partitions = Vec::new();
    partitions
        .try_reserve_exact(targets.len())
        .map_err(|_| AddPartitionsToTxnResponseFailure::RetainedBytes)?;
    for target in targets {
        let topic = matching_topic(target.topic(), &response.results_by_topic_v3_and_below)?;
        let partition = matching_partition(target.partition(), &topic.results_by_partition)?;
        let outcome = match transaction_broker_error(partition.partition_error_code) {
            Some(error) => AddPartitionsToTxnPartitionOutcome::Rejected(error),
            None => AddPartitionsToTxnPartitionOutcome::Added,
        };
        partitions.push(AddPartitionsToTxnPartitionResultRef {
            #[cfg(test)]
            topic: target.topic(),
            #[cfg(test)]
            partition: target.partition(),
            #[cfg(not(test))]
            _target: core::marker::PhantomData,
            outcome,
        });
    }
    Ok(ValidatedAddPartitionsToTxnResponse { partitions })
}

fn validate_v3_shape(
    targets: &[TransactionPartitionRef<'_>],
    response: &AddPartitionsToTxnResponse,
) -> Result<(), AddPartitionsToTxnResponseFailure> {
    if response.error_code != 0 {
        return Err(AddPartitionsToTxnResponseFailure::UnexpectedTopLevelError {
            actual: response.error_code,
        });
    }
    if !response.results_by_transaction.is_empty() {
        return Err(AddPartitionsToTxnResponseFailure::UnexpectedTransactionResults);
    }
    let topics = &response.results_by_topic_v3_and_below;
    let expected_topics = unique_topic_count(targets);
    if topics.len() != expected_topics {
        return Err(AddPartitionsToTxnResponseFailure::TopicCount {
            expected: expected_topics,
            actual: topics.len(),
        });
    }
    let mut partition_count = 0usize;
    for (topic_index, topic) in topics.iter().enumerate() {
        if topic.name.is_empty() {
            return Err(AddPartitionsToTxnResponseFailure::EmptyTopic);
        }
        if topic.results_by_partition.is_empty() {
            return Err(AddPartitionsToTxnResponseFailure::EmptyTopicPartitions);
        }
        if topics[..topic_index]
            .iter()
            .any(|previous| previous.name == topic.name)
        {
            return Err(AddPartitionsToTxnResponseFailure::DuplicateTopic);
        }
        for (partition_index, partition) in topic.results_by_partition.iter().enumerate() {
            if partition.partition_index < 0 {
                return Err(AddPartitionsToTxnResponseFailure::NegativePartition {
                    actual: partition.partition_index,
                });
            }
            if topic.results_by_partition[..partition_index]
                .iter()
                .any(|previous| previous.partition_index == partition.partition_index)
            {
                return Err(AddPartitionsToTxnResponseFailure::DuplicatePartition {
                    actual: partition.partition_index,
                });
            }
            partition_count = partition_count
                .checked_add(1)
                .ok_or(AddPartitionsToTxnResponseFailure::RetainedBytes)?;
        }
    }
    if partition_count != targets.len() {
        return Err(AddPartitionsToTxnResponseFailure::PartitionCount {
            expected: targets.len(),
            actual: partition_count,
        });
    }
    Ok(())
}

fn matching_topic<'a>(
    expected: &str,
    topics: &'a [AddPartitionsToTxnTopicResult],
) -> Result<&'a AddPartitionsToTxnTopicResult, AddPartitionsToTxnResponseFailure> {
    topics
        .iter()
        .find(|topic| topic.name.as_str() == expected)
        .ok_or(AddPartitionsToTxnResponseFailure::MissingTopic)
}

fn matching_partition(
    expected: i32,
    partitions: &[AddPartitionsToTxnPartitionResult],
) -> Result<&AddPartitionsToTxnPartitionResult, AddPartitionsToTxnResponseFailure> {
    partitions
        .iter()
        .find(|partition| partition.partition_index == expected)
        .ok_or(AddPartitionsToTxnResponseFailure::MissingPartition { actual: expected })
}
