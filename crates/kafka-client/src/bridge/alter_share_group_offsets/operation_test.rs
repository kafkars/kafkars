//! Ready and runtime-neutral ShareGroup offset-alteration observation tests.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use crate::{
    ErrorKind, KafkaError,
    admin::{AlterShareGroupOffsetsResult, BatchResult},
};

use super::operation::AdminAlterShareGroupOffsets;

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

#[test]
fn wait_and_future_share_one_concrete_result_shape() {
    let result = AlterShareGroupOffsetsResult::new(Duration::ZERO, BatchResult::new(Vec::new()));
    let waited = AdminAlterShareGroupOffsets::ready_for_test(Ok(result))
        .wait()
        .unwrap_or_else(|error| panic!("ready wait should succeed: {error}"));
    assert!(waited.offsets().entries().is_empty());

    let mut operation = AdminAlterShareGroupOffsets::ready_for_test(Err(KafkaError::new(
        ErrorKind::Backpressure,
        "ShareGroup offset alteration capacity is full",
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
    assert_future::<AdminAlterShareGroupOffsets>();
}
