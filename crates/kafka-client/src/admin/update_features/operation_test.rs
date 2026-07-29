//! Named finalized-feature update operation shape tests.

use std::future::Future;

use super::{UpdateFeatures, UpdateFeaturesResult};

fn assert_future<T: Future<Output = Result<UpdateFeaturesResult, crate::KafkaError>>>() {}

#[test]
fn operation_is_a_named_runtime_neutral_future() {
    assert_future::<UpdateFeatures>();
}
