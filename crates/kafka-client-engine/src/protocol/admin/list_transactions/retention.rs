//! Checked retained-capacity accounting for normalized API-key 66 facts.

use core::mem::size_of;

use kafka_wire::ListTransactionsResponse;

use super::{ListTransactionsProtocolFailure, ListTransactionsResponseFacts, ListedTransaction};

pub(super) const fn broker_error_charge() -> usize {
    size_of::<ListTransactionsResponseFacts>()
}

pub(super) fn source_success_charge(response: &ListTransactionsResponse) -> Option<usize> {
    let unknown_owners = response
        .unknown_state_filters
        .len()
        .checked_mul(size_of::<String>())?;
    let transaction_owners = response
        .transaction_states
        .len()
        .checked_mul(size_of::<ListedTransaction>())?;
    let text = response
        .unknown_state_filters
        .iter()
        .try_fold(0usize, |bytes, state| bytes.checked_add(state.len()))?;
    let text = response
        .transaction_states
        .iter()
        .try_fold(text, |bytes, transaction| {
            bytes
                .checked_add(transaction.transactional_id.len())?
                .checked_add(transaction.transaction_state.len())
        })?;
    size_of::<ListTransactionsResponseFacts>()
        .checked_add(unknown_owners)?
        .checked_add(transaction_owners)?
        .checked_add(text)
}

pub(super) fn normalized_success_charge(
    unknown_states: &[String],
    transactions: &[ListedTransaction],
) -> Option<usize> {
    let unknown_owners = unknown_states.len().checked_mul(size_of::<String>())?;
    let transaction_owners = transactions
        .len()
        .checked_mul(size_of::<ListedTransaction>())?;
    let text = unknown_states
        .iter()
        .try_fold(0usize, |bytes, state| bytes.checked_add(state.capacity()))?;
    let text = transactions.iter().try_fold(text, |bytes, transaction| {
        bytes.checked_add(transaction.retained_text_bytes()?)
    })?;
    size_of::<ListTransactionsResponseFacts>()
        .checked_add(unknown_owners)?
        .checked_add(transaction_owners)?
        .checked_add(text)
}

pub(super) fn ensure_limit(
    required: usize,
    limit: usize,
) -> Result<(), ListTransactionsProtocolFailure> {
    if required > limit {
        return Err(ListTransactionsProtocolFailure::RetainedBytes { required, limit });
    }
    Ok(())
}
