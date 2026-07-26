//! Private assigned-event observer shape scenarios.

use std::future::Future;

use super::next_event::AssignedConsumerNextEvent;
use crate::{KafkaError, consumer::AssignedConsumerEvent};

#[test]
fn bridge_next_event_is_one_send_runtime_neutral_observer() {
    fn require<T: Future<Output = Result<Option<AssignedConsumerEvent>, KafkaError>> + Send>() {}
    require::<AssignedConsumerNextEvent<'static>>();
}
