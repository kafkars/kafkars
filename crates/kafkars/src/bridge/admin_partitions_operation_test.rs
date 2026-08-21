//! Ready and runtime-neutral `CreatePartitions` observation scenarios.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};

use super::admin_partitions_operation::AdminCreatePartitions;
use crate::{ErrorKind, KafkaError, admin::BatchResult};

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

#[test]
fn wait_and_future_share_one_concrete_result_shape() {
    let waited = AdminCreatePartitions::ready_for_test(Ok(BatchResult::new(Vec::new())))
        .wait()
        .unwrap_or_else(|error| panic!("ready wait should succeed: {error}"));
    assert!(waited.entries().is_empty());
    let mut operation = AdminCreatePartitions::ready_for_test(Err(KafkaError::new(
        ErrorKind::Backpressure,
        "partition capacity is full",
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
fn private_create_partitions_operation_is_send_without_runtime_dependencies() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<AdminCreatePartitions>();
}
