//! Public classic-group event-observation operation contract.

use std::future::Future;

use super::{Consumer, ConsumerEvent, NextConsumerEvent};
use crate::KafkaError;

#[test]
fn next_event_is_send_linear_and_borrows_the_unique_consumer() {
    fn require<T: Future<Output = Result<Option<ConsumerEvent>, KafkaError>> + Send>() {}
    fn borrow(consumer: &mut Consumer) -> NextConsumerEvent<'_> {
        consumer.next_event()
    }
    fn require_borrow(_borrow: for<'a> fn(&'a mut Consumer) -> NextConsumerEvent<'a>) {}

    require::<NextConsumerEvent<'static>>();
    require_borrow(borrow);
}

#[test]
fn immediate_event_observation_borrows_the_unique_consumer() {
    fn require_take(_take: fn(&mut Consumer) -> Result<Option<ConsumerEvent>, KafkaError>) {}

    require_take(Consumer::try_take_event);
}
