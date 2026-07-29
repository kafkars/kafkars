//! Fallible copying, duplicate rejection, and canonical ordering for API key 66.

use kafka_wire::ListTransactionsResponse;

use super::{
    ListTransactionsProtocolFailure, ListTransactionsResponseFacts, ListedTransaction,
    retention::{ensure_limit, normalized_success_charge},
};

pub(super) fn materialize_success(
    throttle_time_ms: u32,
    response: &ListTransactionsResponse,
    source_required: usize,
    retained_limit: usize,
) -> Result<ListTransactionsResponseFacts, ListTransactionsProtocolFailure> {
    let mut unknown_states = Vec::new();
    unknown_states
        .try_reserve_exact(response.unknown_state_filters.len())
        .map_err(|_| ListTransactionsProtocolFailure::Allocation {
            field: "unknown_state_filters",
            requested: response.unknown_state_filters.len(),
        })?;
    for state in &response.unknown_state_filters {
        unknown_states.push(copy_string(
            state.as_str(),
            "unknown_state_filter",
            retained_limit,
        )?);
    }
    unknown_states.sort_unstable();
    if unknown_states.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(ListTransactionsProtocolFailure::DuplicateUnknownStateFilter);
    }

    let mut transactions = Vec::new();
    transactions
        .try_reserve_exact(response.transaction_states.len())
        .map_err(|_| ListTransactionsProtocolFailure::Allocation {
            field: "transactions",
            requested: response.transaction_states.len(),
        })?;
    for transaction in &response.transaction_states {
        transactions.push(ListedTransaction::new(
            copy_string(
                transaction.transactional_id.as_str(),
                "transactional_id",
                retained_limit,
            )?,
            transaction.producer_id,
            copy_string(
                transaction.transaction_state.as_str(),
                "transaction_state",
                retained_limit,
            )?,
        ));
    }
    transactions
        .sort_unstable_by(|left, right| left.transactional_id().cmp(right.transactional_id()));
    if transactions
        .windows(2)
        .any(|pair| pair[0].transactional_id() == pair[1].transactional_id())
    {
        return Err(ListTransactionsProtocolFailure::DuplicateTransactionalId);
    }

    let normalized =
        normalized_success_charge(&unknown_states, &transactions).unwrap_or(usize::MAX);
    ensure_limit(normalized, retained_limit)?;
    Ok(ListTransactionsResponseFacts::new(
        throttle_time_ms,
        None,
        unknown_states,
        transactions,
        source_required.max(normalized),
    ))
}

fn copy_string(
    source: &str,
    field: &'static str,
    retained_limit: usize,
) -> Result<String, ListTransactionsProtocolFailure> {
    let mut owned = String::new();
    owned.try_reserve_exact(source.len()).map_err(|_| {
        ListTransactionsProtocolFailure::Allocation {
            field,
            requested: source.len(),
        }
    })?;
    owned.push_str(source);
    if owned.capacity() > retained_limit {
        return Err(ListTransactionsProtocolFailure::RetainedBytes {
            required: owned.capacity(),
            limit: retained_limit,
        });
    }
    Ok(owned)
}
