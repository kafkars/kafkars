//! Bounded canonicalization for transaction-description response facts.

use super::{
    AdminDescribeTransactionDescription, AdminDescribeTransactionOutcome,
    AdminDescribeTransactionsMachine, DESCRIBE_TRANSACTIONS_MAX_PARTITIONS,
    DESCRIBE_TRANSACTIONS_MAX_TOPIC_BYTES, DESCRIBE_TRANSACTIONS_MAX_TOPICS,
};

const MAX_TOPIC_NAME_BYTES: usize = 249;

#[derive(Clone, Copy)]
pub(super) struct RetainedCounts {
    pub(super) topics: usize,
    pub(super) partitions: usize,
    pub(super) topic_bytes: usize,
}

impl RetainedCounts {
    pub(super) const fn from_machine(machine: &AdminDescribeTransactionsMachine) -> Self {
        Self {
            topics: machine.topic_count,
            partitions: machine.partition_count,
            topic_bytes: machine.topic_bytes,
        }
    }
}

pub(super) fn normalize_outcome(
    outcome: &mut AdminDescribeTransactionOutcome,
    counts: RetainedCounts,
) -> Option<RetainedCounts> {
    let Some(description) = outcome.description_mut() else {
        return Some(counts);
    };
    normalize_description(description, counts)
}

fn normalize_description(
    description: &mut AdminDescribeTransactionDescription,
    mut counts: RetainedCounts,
) -> Option<RetainedCounts> {
    if !description.has_bounded_scalar_shape() {
        return None;
    }
    counts.topics = counts.topics.checked_add(description.topics().len())?;
    if counts.topics > DESCRIBE_TRANSACTIONS_MAX_TOPICS {
        return None;
    }
    for topic in description.topics_mut() {
        if topic.topic().is_empty() || topic.topic().len() > MAX_TOPIC_NAME_BYTES {
            return None;
        }
        counts.topic_bytes = counts.topic_bytes.checked_add(topic.topic().len())?;
        if counts.topic_bytes > DESCRIBE_TRANSACTIONS_MAX_TOPIC_BYTES {
            return None;
        }
        if topic.partitions().is_empty() {
            return None;
        }
        counts.partitions = counts.partitions.checked_add(topic.partitions().len())?;
        if counts.partitions > DESCRIBE_TRANSACTIONS_MAX_PARTITIONS {
            return None;
        }
        topic.partitions_mut().sort_unstable();
        if topic.partitions().iter().any(|partition| *partition < 0)
            || topic.partitions().windows(2).any(|pair| pair[0] == pair[1])
        {
            return None;
        }
    }
    description
        .topics_mut()
        .sort_unstable_by(|left, right| left.topic().cmp(right.topic()));
    if description
        .topics()
        .windows(2)
        .any(|pair| pair[0].topic() == pair[1].topic())
    {
        return None;
    }
    Some(counts)
}
