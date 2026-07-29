//! Fallible materialization and canonical ordering for API-key 65 success.

use kafka_wire::describe_transactions_response::{TopicData, TransactionState};

use super::{
    DescribeTransactionsProtocolFailure, NormalizedDescribeTransactionDescription,
    NormalizedDescribeTransactionTopic,
    retention::{ensure_limit, normalized_success_charge},
};

pub(super) fn materialize_success(
    state: &TransactionState,
    required: usize,
    retained_limit: usize,
) -> Result<(NormalizedDescribeTransactionDescription, usize), DescribeTransactionsProtocolFailure>
{
    let transaction_state = copy_string(
        state.transaction_state.as_str(),
        "transaction_state",
        retained_limit,
    )?;
    let mut topics = Vec::new();
    topics.try_reserve_exact(state.topics.len()).map_err(|_| {
        DescribeTransactionsProtocolFailure::Allocation {
            field: "topics",
            requested: state.topics.len(),
        }
    })?;
    for topic in &state.topics {
        topics.push(materialize_topic(topic, retained_limit)?);
    }
    topics.sort_unstable_by(|left, right| left.topic().cmp(right.topic()));
    if topics
        .windows(2)
        .any(|pair| pair[0].topic() == pair[1].topic())
    {
        return Err(DescribeTransactionsProtocolFailure::DuplicateTopic);
    }
    let description = NormalizedDescribeTransactionDescription::new(
        transaction_state,
        state.transaction_timeout_ms,
        (state.transaction_start_time_ms != -1).then_some(state.transaction_start_time_ms),
        state.producer_id,
        state.producer_epoch,
        topics,
    );
    let retained = normalized_success_charge(&description).unwrap_or(usize::MAX);
    ensure_limit(retained, retained_limit)?;
    Ok((description, required.max(retained)))
}

fn materialize_topic(
    source: &TopicData,
    retained_limit: usize,
) -> Result<NormalizedDescribeTransactionTopic, DescribeTransactionsProtocolFailure> {
    let topic = copy_string(source.topic.as_str(), "topic", retained_limit)?;
    let mut partitions = Vec::new();
    partitions
        .try_reserve_exact(source.partitions.len())
        .map_err(|_| DescribeTransactionsProtocolFailure::Allocation {
            field: "partitions",
            requested: source.partitions.len(),
        })?;
    partitions.extend_from_slice(&source.partitions);
    partitions.sort_unstable();
    if let Some(pair) = partitions.windows(2).find(|pair| pair[0] == pair[1]) {
        return Err(DescribeTransactionsProtocolFailure::DuplicatePartition { actual: pair[0] });
    }
    Ok(NormalizedDescribeTransactionTopic::new(topic, partitions))
}

fn copy_string(
    source: &str,
    field: &'static str,
    retained_limit: usize,
) -> Result<String, DescribeTransactionsProtocolFailure> {
    let mut owned = String::new();
    owned.try_reserve_exact(source.len()).map_err(|_| {
        DescribeTransactionsProtocolFailure::Allocation {
            field,
            requested: source.len(),
        }
    })?;
    owned.push_str(source);
    if owned.capacity() > retained_limit {
        return Err(DescribeTransactionsProtocolFailure::RetainedBytes {
            required: owned.capacity(),
            limit: retained_limit,
        });
    }
    Ok(owned)
}
