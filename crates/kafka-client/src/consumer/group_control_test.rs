//! Public hosted pause and resume shape contract.

use super::{Consumer, TopicPartition};
use crate::KafkaError;

#[test]
fn partition_controls_are_batch_synchronous_and_timeout_free() {
    fn require(_control: fn(&mut Consumer, &[TopicPartition]) -> Result<(), KafkaError>) {}

    require(Consumer::pause);
    require(Consumer::resume);
}
