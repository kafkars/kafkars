//! Builder remains an inert, thread-safe owner with the complete option surface.

use std::time::Duration;

use super::{ListTransactions, ListTransactionsBuilder};

#[test]
fn builder_is_send_and_exposes_inert_filter_and_submission_methods() {
    fn assert_send<T: Send>() {}
    assert_send::<ListTransactionsBuilder>();

    let states: fn(ListTransactionsBuilder, Vec<String>) -> ListTransactionsBuilder =
        ListTransactionsBuilder::state_filters::<Vec<String>, String>;
    let producers: fn(ListTransactionsBuilder, Vec<i64>) -> ListTransactionsBuilder =
        ListTransactionsBuilder::producer_id_filters::<Vec<i64>>;
    let duration: fn(ListTransactionsBuilder, Duration) -> ListTransactionsBuilder =
        ListTransactionsBuilder::duration_filter;
    let pattern: fn(ListTransactionsBuilder, String) -> ListTransactionsBuilder =
        ListTransactionsBuilder::transactional_id_pattern;
    let deadline: fn(ListTransactionsBuilder, Duration) -> ListTransactionsBuilder =
        ListTransactionsBuilder::deadline_after;
    let submit: fn(ListTransactionsBuilder) -> ListTransactions = ListTransactionsBuilder::submit;

    let _ = (states, producers, duration, pattern, deadline, submit);
}
