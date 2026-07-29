//! Bounded accumulation and deterministic cross-broker deduplication.

use super::{
    AdminListedTransaction, LIST_TRANSACTIONS_MAX_RESULT_STRING_BYTES,
    LIST_TRANSACTIONS_MAX_TRANSACTION_STATE_BYTES, LIST_TRANSACTIONS_MAX_TRANSACTIONAL_ID_BYTES,
    LIST_TRANSACTIONS_MAX_TRANSACTIONS, LIST_TRANSACTIONS_MAX_UNKNOWN_STATE_FILTERS,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RetainedCounts {
    pub(super) unknown_state_filters: usize,
    pub(super) transactions: usize,
    pub(super) string_bytes: usize,
}

impl RetainedCounts {
    pub(super) const fn new(
        unknown_state_filters: usize,
        transactions: usize,
        string_bytes: usize,
    ) -> Self {
        Self {
            unknown_state_filters,
            transactions,
            string_bytes,
        }
    }
}

pub(super) fn retain_listing(
    unknown_state_filters: &[String],
    transactions: &[AdminListedTransaction],
    retained: RetainedCounts,
) -> Option<RetainedCounts> {
    let unknown_count = retained
        .unknown_state_filters
        .checked_add(unknown_state_filters.len())?;
    let transaction_count = retained.transactions.checked_add(transactions.len())?;
    if unknown_count > LIST_TRANSACTIONS_MAX_UNKNOWN_STATE_FILTERS
        || transaction_count > LIST_TRANSACTIONS_MAX_TRANSACTIONS
    {
        return None;
    }

    let mut string_bytes = retained.string_bytes;
    for state in unknown_state_filters {
        if state.len() > LIST_TRANSACTIONS_MAX_TRANSACTION_STATE_BYTES {
            return None;
        }
        string_bytes = retain_bytes(string_bytes, state.len())?;
    }
    for transaction in transactions {
        if transaction.transactional_id().len() > LIST_TRANSACTIONS_MAX_TRANSACTIONAL_ID_BYTES
            || transaction.transaction_state().len() > LIST_TRANSACTIONS_MAX_TRANSACTION_STATE_BYTES
        {
            return None;
        }
        string_bytes = retain_bytes(string_bytes, transaction.transactional_id().len())?;
        string_bytes = retain_bytes(string_bytes, transaction.transaction_state().len())?;
    }
    Some(RetainedCounts::new(
        unknown_count,
        transaction_count,
        string_bytes,
    ))
}

fn retain_bytes(current: usize, additional: usize) -> Option<usize> {
    let retained = current.checked_add(additional)?;
    (retained <= LIST_TRANSACTIONS_MAX_RESULT_STRING_BYTES).then_some(retained)
}

pub(super) fn canonicalize(
    unknown_state_filters: &mut Vec<String>,
    transactions: &mut Vec<AdminListedTransaction>,
) -> bool {
    unknown_state_filters.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    unknown_state_filters.dedup();

    transactions.sort_unstable_by(compare_transaction);
    let mut contradictory = false;
    transactions.dedup_by(|right, left| {
        if left.transactional_id() != right.transactional_id() {
            return false;
        }
        contradictory |= left != right;
        true
    });
    !contradictory
}

fn compare_transaction(
    left: &AdminListedTransaction,
    right: &AdminListedTransaction,
) -> core::cmp::Ordering {
    left.transactional_id()
        .as_bytes()
        .cmp(right.transactional_id().as_bytes())
        .then_with(|| left.producer_id().cmp(&right.producer_id()))
        .then_with(|| {
            left.transaction_state()
                .as_bytes()
                .cmp(right.transaction_state().as_bytes())
        })
}
