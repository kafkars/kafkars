//! Trait contract for private assigned-consumer close observation.

use std::future::Future;

use super::close::AssignedConsumerClose;
use crate::KafkaError;

#[test]
fn private_close_is_a_send_future_without_an_async_runtime() {
    fn require<T: Future<Output = Result<(), KafkaError>> + Send>() {}

    require::<AssignedConsumerClose>();
}
