//! Public partition-transaction abort builder shape tests.

use std::time::Duration;

use super::{AbortPartitionTransaction, AbortTransactionBuilder};

#[test]
fn builder_keeps_timeout_and_request_inert_until_submit() {
    let deadline_after: fn(AbortTransactionBuilder, Duration) -> AbortTransactionBuilder =
        AbortTransactionBuilder::deadline_after;
    let submit: fn(AbortTransactionBuilder) -> AbortPartitionTransaction =
        AbortTransactionBuilder::submit;

    let _ = (deadline_after, submit);
}
