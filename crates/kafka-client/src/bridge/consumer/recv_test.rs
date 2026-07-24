//! Private receive bridge shape scenarios.

use std::future::Future;

use super::{AssignedConsumerBatch, recv::AssignedConsumerRecv};
use crate::KafkaError;

#[test]
fn bridge_receive_is_one_send_runtime_neutral_observer() {
    fn require<T: Future<Output = Result<Option<AssignedConsumerBatch>, KafkaError>> + Send>() {}
    require::<AssignedConsumerRecv<'static>>();
}
