//! Structural correlation for one broker-aggregated Produce response.

use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    sync::Arc,
};

use kafka_client_core::ProducerBatchSuccess;
use kafka_wire::ProduceResponse;

use super::produce_response::{
    ProduceResponseFailure, ProduceResponseProtocolFailure, normalize_partition_response,
};

const UNSEEN: (usize, usize) = (usize::MAX, usize::MAX);

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct BatchedProduceResponseIndex {
    targets: HashMap<Arc<str>, HashMap<i32, (usize, usize)>>,
}

/// Proves the generated response is a bijection over the submitted partitions.
pub(crate) fn validate_batched_produce_response(
    response: &ProduceResponse,
    expected_count: usize,
    expected_targets: impl IntoIterator<Item = (Arc<str>, i32)>,
) -> Result<BatchedProduceResponseIndex, ProduceResponseFailure> {
    let mut expected: HashMap<Arc<str>, HashMap<i32, (usize, usize)>> = HashMap::new();
    expected
        .try_reserve(expected_count)
        .map_err(|_| capacity(expected_count))?;
    let mut inserted = 0usize;
    for (topic, partition) in expected_targets {
        let partitions = match expected.entry(topic) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(HashMap::new()),
        };
        partitions
            .try_reserve(1)
            .map_err(|_| capacity(expected_count))?;
        if partitions.insert(partition, UNSEEN).is_some() {
            return Err(mismatch());
        }
        inserted = inserted.checked_add(1).ok_or_else(mismatch)?;
    }
    if inserted != expected_count {
        return Err(mismatch());
    }

    let actual_count = response
        .responses
        .iter()
        .map(|topic| topic.partition_responses.len())
        .try_fold(0usize, usize::checked_add)
        .ok_or_else(mismatch)?;
    if actual_count != expected_count {
        return Err(ProduceResponseFailure::protocol(
            ProduceResponseProtocolFailure::BatchedPartitionCount {
                expected: expected_count,
                actual: actual_count,
            },
        ));
    }
    let mut seen_topics = HashSet::new();
    seen_topics
        .try_reserve(expected.len())
        .map_err(|_| capacity(expected_count))?;
    for (topic_index, topic) in response.responses.iter().enumerate() {
        if !seen_topics.insert(topic.name.as_str()) {
            return Err(mismatch());
        }
        let Some(expected_partitions) = expected.get_mut(topic.name.as_str()) else {
            return Err(mismatch());
        };
        for (partition_index, partition) in topic.partition_responses.iter().enumerate() {
            let Some(location) = expected_partitions.get_mut(&partition.index) else {
                return Err(mismatch());
            };
            if *location != UNSEEN {
                return Err(mismatch());
            }
            *location = (topic_index, partition_index);
        }
    }
    if expected
        .values()
        .flat_map(HashMap::values)
        .any(|location| *location == UNSEEN)
    {
        return Err(mismatch());
    }
    Ok(BatchedProduceResponseIndex { targets: expected })
}

/// Normalizes one exact partition after whole-response correlation succeeds.
pub(crate) fn normalize_batched_produce_partition(
    response: &ProduceResponse,
    index: &BatchedProduceResponseIndex,
    topic: &str,
    partition: i32,
) -> Result<ProducerBatchSuccess, ProduceResponseFailure> {
    let Some((topic_index, partition_index)) = index
        .targets
        .get(topic)
        .and_then(|partitions| partitions.get(&partition))
        .copied()
    else {
        return Err(mismatch());
    };
    let partition = response
        .responses
        .get(topic_index)
        .and_then(|topic| topic.partition_responses.get(partition_index))
        .unwrap_or_else(|| unreachable!("validated Produce response index remains exact"));
    normalize_partition_response(partition)
}

const fn mismatch() -> ProduceResponseFailure {
    ProduceResponseFailure::protocol(ProduceResponseProtocolFailure::BatchedTargetMismatch)
}

const fn capacity(requested: usize) -> ProduceResponseFailure {
    ProduceResponseFailure::protocol(ProduceResponseProtocolFailure::BatchedCorrelationCapacity {
        requested,
    })
}
