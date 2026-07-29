//! Exact feature floors for flexible `ListTransactions` versions zero through two.

use super::ListTransactionsRequestPlan;

pub(crate) const LIST_TRANSACTIONS_MIN_VERSION: i16 = 0;
pub(crate) const LIST_TRANSACTIONS_MAX_VERSION: i16 = 2;

pub(super) const fn supports_list_transactions_version(version: i16) -> bool {
    version >= LIST_TRANSACTIONS_MIN_VERSION && version <= LIST_TRANSACTIONS_MAX_VERSION
}

pub(super) const fn list_transactions_version_floor(plan: ListTransactionsRequestPlan<'_>) -> i16 {
    if plan.transactional_id_pattern().is_some() {
        2
    } else if plan.duration_filter_ms().is_some() {
        1
    } else {
        0
    }
}
