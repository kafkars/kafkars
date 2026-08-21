//! Public lossless group-consumer build error contracts.

use super::{ConsumerBuildError, ConsumerBuilder};
use crate::KafkaError;

#[test]
fn build_error_is_concrete_and_returns_both_owned_parts() {
    fn require_error<T: std::error::Error>() {}
    fn accessors(error: ConsumerBuildError) {
        let _: &ConsumerBuilder = error.builder();
        let _: &KafkaError = error.error();
        let _: (ConsumerBuilder, KafkaError) = error.into_parts();
    }
    require_error::<ConsumerBuildError>();
    let _ = accessors as fn(ConsumerBuildError);
}
