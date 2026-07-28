//! Public lossless group close rejection contract.

use super::{CloseConsumer, Consumer, ConsumerCloseAdmissionError};
use crate::KafkaError;

#[test]
fn rejection_exposes_and_returns_the_exact_consumer() {
    fn contract(error: ConsumerCloseAdmissionError) -> (Consumer, KafkaError) {
        let _: &Consumer = error.consumer();
        let _: &KafkaError = error.error();
        error.into_parts()
    }
    let _ = contract as fn(ConsumerCloseAdmissionError) -> (Consumer, KafkaError);
    let _: fn(Consumer) -> Result<CloseConsumer, ConsumerCloseAdmissionError> = Consumer::try_close;
}
