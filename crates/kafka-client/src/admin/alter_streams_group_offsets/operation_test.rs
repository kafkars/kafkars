//! Named `StreamsGroup` offset-alteration observer tests.

use std::future::Future;

use super::{AlterStreamsGroupOffsets, AlterStreamsGroupOffsetsResult};

#[test]
fn operation_is_a_send_runtime_neutral_future_with_typed_output() {
    fn assert_operation<T>()
    where
        T: Send
            + std::fmt::Debug
            + Future<Output = Result<AlterStreamsGroupOffsetsResult, crate::KafkaError>>,
    {
    }

    assert_operation::<AlterStreamsGroupOffsets>();
}
