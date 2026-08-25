//! Transaction-end intent remains a closed public commit-or-abort domain.

use super::TransactionEndIntent;

#[test]
fn commit_and_abort_intents_are_distinct() {
    assert_ne!(TransactionEndIntent::Commit, TransactionEndIntent::Abort);
}
