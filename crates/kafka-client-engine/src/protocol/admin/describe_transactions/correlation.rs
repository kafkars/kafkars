//! Exact single-ID response correlation before scalar interpretation.

use kafka_wire::{DescribeTransactionsResponse, describe_transactions_response::TransactionState};

use super::DescribeTransactionsProtocolFailure;

pub(super) fn correlated_state<'a>(
    transactional_id: &str,
    response: &'a DescribeTransactionsResponse,
) -> Result<&'a TransactionState, DescribeTransactionsProtocolFailure> {
    let [state] = response.transaction_states.as_slice() else {
        return Err(
            DescribeTransactionsProtocolFailure::UnexpectedTransactionStateCount {
                actual: response.transaction_states.len(),
            },
        );
    };
    if state.transactional_id.as_str() != transactional_id {
        return Err(DescribeTransactionsProtocolFailure::UnexpectedTransactionalId);
    }
    Ok(state)
}
