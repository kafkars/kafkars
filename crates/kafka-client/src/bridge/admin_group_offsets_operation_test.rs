//! Ready and runtime-neutral group-offset observation scenarios.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use super::admin_group_offsets_operation::AdminListConsumerGroupOffsets;
use crate::{
    ErrorKind, KafkaError,
    admin::{BatchResult, ListConsumerGroupOffsetsResult},
};

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

#[test]
fn wait_and_future_share_one_concrete_result_shape() {
    let result = ListConsumerGroupOffsetsResult::new(Duration::ZERO, BatchResult::new(Vec::new()));
    let waited = AdminListConsumerGroupOffsets::ready_for_test(Ok(result))
        .wait()
        .unwrap_or_else(|error| panic!("ready wait should succeed: {error}"));
    assert!(waited.offsets().entries().is_empty());

    let mut operation = AdminListConsumerGroupOffsets::ready_for_test(Err(KafkaError::new(
        ErrorKind::Backpressure,
        "group-offset capacity is full",
    )));
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut operation).poll(&mut context),
        Poll::Ready(Err(error)) if error.kind() == ErrorKind::Backpressure
    ));
    assert!(matches!(
        Pin::new(&mut operation).poll(&mut context),
        Poll::Ready(Err(error)) if error.kind() == ErrorKind::State
    ));
}

#[test]
fn private_operation_is_a_send_future_without_a_runtime() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<AdminListConsumerGroupOffsets>();
}
