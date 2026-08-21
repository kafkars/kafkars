//! Streams-group offset-deletion observer ownership tests.

use std::future::Future;

use super::{DeleteStreamsGroupOffsets, DeleteStreamsGroupOffsetsResult};

#[test]
fn operation_is_one_named_runtime_neutral_send_future() {
    fn assert_operation<T>()
    where
        T: Future<Output = Result<DeleteStreamsGroupOffsetsResult, crate::KafkaError>>
            + Send
            + std::fmt::Debug,
    {
    }

    assert_operation::<DeleteStreamsGroupOffsets>();
}
