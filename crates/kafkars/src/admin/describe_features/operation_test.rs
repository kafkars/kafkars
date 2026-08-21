//! Named Kafka feature discovery operation shape tests.

use std::future::Future;

use super::{DescribeFeatures, DescribeFeaturesResult};

fn assert_future<T: Future<Output = Result<DescribeFeaturesResult, crate::KafkaError>>>() {}

#[test]
fn operation_is_a_named_runtime_neutral_future() {
    assert_future::<DescribeFeatures>();
}
