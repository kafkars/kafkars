//! Named metadata-quorum voter-addition operation shape tests.

use std::future::Future;

use super::{AddRaftVoter, AddRaftVoterResult};

fn assert_future<T: Future<Output = Result<AddRaftVoterResult, crate::KafkaError>>>() {}

#[test]
fn operation_is_a_named_runtime_neutral_future() {
    assert_future::<AddRaftVoter>();
}
