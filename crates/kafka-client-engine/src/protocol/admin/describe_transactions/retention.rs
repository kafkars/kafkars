//! Fallible retained-capacity accounting for normalized API-key 65 facts.

use core::mem::size_of;

use kafka_wire::describe_transactions_response::TransactionState;

use super::{
    DescribeTransactionsProtocolFailure, NormalizedDescribeTransactionDescription,
    NormalizedDescribeTransactionTopic, NormalizedDescribeTransactionsResponse,
};

pub(super) const fn error_charge() -> usize {
    size_of::<NormalizedDescribeTransactionsResponse>()
}

pub(super) fn source_success_charge(state: &TransactionState) -> Option<usize> {
    let mut required = size_of::<NormalizedDescribeTransactionsResponse>()
        .checked_add(state.transaction_state.len())?
        .checked_add(
            state
                .topics
                .len()
                .checked_mul(size_of::<NormalizedDescribeTransactionTopic>())?,
        )?;
    for topic in &state.topics {
        required = required
            .checked_add(topic.topic.len())?
            .checked_add(topic.partitions.len().checked_mul(size_of::<i32>())?)?;
    }
    Some(required)
}

pub(super) fn normalized_success_charge(
    description: &NormalizedDescribeTransactionDescription,
) -> Option<usize> {
    let mut required = size_of::<NormalizedDescribeTransactionsResponse>()
        .checked_add(description.transaction_state().len())?
        .checked_add(
            description
                .topics()
                .len()
                .checked_mul(size_of::<NormalizedDescribeTransactionTopic>())?,
        )?;
    for topic in description.topics() {
        required = required
            .checked_add(topic.topic().len())?
            .checked_add(topic.partitions().len().checked_mul(size_of::<i32>())?)?;
    }
    Some(required)
}

pub(super) fn ensure_limit(
    required: usize,
    limit: usize,
) -> Result<(), DescribeTransactionsProtocolFailure> {
    if required > limit {
        return Err(DescribeTransactionsProtocolFailure::RetainedBytes { required, limit });
    }
    Ok(())
}
