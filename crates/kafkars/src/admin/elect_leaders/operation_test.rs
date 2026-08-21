//! Named runtime-neutral leader-election operation shape tests.

use std::future::Future;

use crate::KafkaError;

use super::{ElectLeaders, ElectLeadersResult};

fn assert_send_future<T>()
where
    T: Future<Output = Result<ElectLeadersResult, KafkaError>> + Send,
{
}

#[test]
fn operation_is_one_named_send_future() {
    assert_send_future::<ElectLeaders>();
}
