//! Public lossless assigned-consumer build error contracts.

use super::{AssignedConsumerBuildError, AssignedConsumerBuilder};
use crate::KafkaError;

#[test]
fn build_error_is_concrete_and_returns_both_owned_parts() {
    fn require_error<T: std::error::Error>() {}
    fn accessors(error: AssignedConsumerBuildError) {
        let _: &AssignedConsumerBuilder = error.builder();
        let _: &KafkaError = error.error();
        let _: (AssignedConsumerBuilder, KafkaError) = error.into_parts();
    }

    require_error::<AssignedConsumerBuildError>();
    let _ = accessors as fn(AssignedConsumerBuildError);
}
