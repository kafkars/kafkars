//! Generated v3 `AddPartitionsToTxn` request construction.

use kafka_wire::{
    AddPartitionsToTxnRequest, add_partitions_to_txn_request::AddPartitionsToTxnTopic,
};

/// One caller-owned transaction partition borrowed during wire adaptation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TransactionPartitionRef<'a> {
    topic: &'a str,
    partition: i32,
}

impl<'a> TransactionPartitionRef<'a> {
    pub(crate) const fn new(topic: &'a str, partition: i32) -> Self {
        Self { topic, partition }
    }

    pub(crate) const fn topic(self) -> &'a str {
        self.topic
    }

    pub(crate) const fn partition(self) -> i32 {
        self.partition
    }
}

/// Request facts that cannot safely enter the generated v3 shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AddPartitionsToTxnRequestFailure {
    EmptyTargets,
    EmptyTopic,
    NegativePartition { actual: i32 },
    DuplicateTarget { partition: i32 },
    RetainedBytes,
}

/// Builds one v3 request without coordinator routing or retry policy.
pub(crate) fn add_partitions_to_txn_v3_request(
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    targets: &[TransactionPartitionRef<'_>],
) -> Result<AddPartitionsToTxnRequest, AddPartitionsToTxnRequestFailure> {
    validate_targets(targets)?;
    let mut request = AddPartitionsToTxnRequest::default();
    request.v3_and_below_transactional_id = transactional_id.into();
    request.v3_and_below_producer_id = producer_id;
    request.v3_and_below_producer_epoch = producer_epoch;
    request
        .v3_and_below_topics
        .try_reserve_exact(unique_topic_count(targets))
        .map_err(|_| AddPartitionsToTxnRequestFailure::RetainedBytes)?;
    for target in targets {
        append_target(&mut request.v3_and_below_topics, *target)?;
    }
    Ok(request)
}

fn validate_targets(
    targets: &[TransactionPartitionRef<'_>],
) -> Result<(), AddPartitionsToTxnRequestFailure> {
    if targets.is_empty() {
        return Err(AddPartitionsToTxnRequestFailure::EmptyTargets);
    }
    for (index, target) in targets.iter().enumerate() {
        if target.topic.is_empty() {
            return Err(AddPartitionsToTxnRequestFailure::EmptyTopic);
        }
        if target.partition < 0 {
            return Err(AddPartitionsToTxnRequestFailure::NegativePartition {
                actual: target.partition,
            });
        }
        if targets[..index].iter().any(|previous| {
            previous.topic == target.topic && previous.partition == target.partition
        }) {
            return Err(AddPartitionsToTxnRequestFailure::DuplicateTarget {
                partition: target.partition,
            });
        }
    }
    Ok(())
}

pub(super) fn unique_topic_count(targets: &[TransactionPartitionRef<'_>]) -> usize {
    targets
        .iter()
        .enumerate()
        .filter(|(index, target)| {
            !targets[..*index]
                .iter()
                .any(|previous| previous.topic == target.topic)
        })
        .count()
}

fn append_target(
    topics: &mut Vec<AddPartitionsToTxnTopic>,
    target: TransactionPartitionRef<'_>,
) -> Result<(), AddPartitionsToTxnRequestFailure> {
    if let Some(topic) = topics
        .iter_mut()
        .find(|topic| topic.name.as_str() == target.topic)
    {
        topic
            .partitions
            .try_reserve_exact(1)
            .map_err(|_| AddPartitionsToTxnRequestFailure::RetainedBytes)?;
        topic.partitions.push(target.partition);
        return Ok(());
    }
    let mut topic = AddPartitionsToTxnTopic::default();
    topic.name = target.topic.into();
    topic
        .partitions
        .try_reserve_exact(1)
        .map_err(|_| AddPartitionsToTxnRequestFailure::RetainedBytes)?;
    topic.partitions.push(target.partition);
    topics.push(topic);
    Ok(())
}
