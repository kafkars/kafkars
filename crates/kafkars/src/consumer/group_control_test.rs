//! Public hosted group-control shape contract.

use super::{Consumer, ConsumerControl, TopicPartition};
use crate::KafkaError;

#[test]
fn partition_controls_are_batch_synchronous_and_timeout_free() {
    fn require(_control: fn(&mut Consumer, &[TopicPartition]) -> Result<(), KafkaError>) {}

    require(Consumer::pause);
    require(Consumer::resume);
}

#[test]
fn consumer_control_is_cloneable_send_and_sync() {
    fn require<T: Clone + Send + Sync>() {}

    require::<ConsumerControl>();
}

#[test]
fn consumer_exposes_control_without_mutable_ownership() {
    fn require_control(_control: fn(&Consumer) -> ConsumerControl) {}
    fn require_shutdown(_shutdown: fn(&ConsumerControl)) {}

    require_control(Consumer::control);
    require_shutdown(ConsumerControl::request_shutdown);
}
