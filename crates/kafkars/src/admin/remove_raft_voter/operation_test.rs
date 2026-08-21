//! Named metadata-quorum voter-removal operation shape tests.

use std::future::Future;

use super::{RemoveRaftVoter, RemoveRaftVoterResult};

fn assert_future<T: Future<Output = Result<RemoveRaftVoterResult, crate::KafkaError>>>() {}

#[test]
fn operation_is_a_named_runtime_neutral_future() {
    assert_future::<RemoveRaftVoter>();
}
