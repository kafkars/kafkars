//! Ready and runtime-neutral named admin observation scenarios.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};

use super::admin_operation::AdminCreateTopics;
use crate::{ErrorKind, KafkaError, admin::BatchResult};

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

#[test]
fn wait_and_future_share_the_same_concrete_result_shape() {
    let waited = AdminCreateTopics::ready_for_test(Ok(BatchResult::new(Vec::new())))
        .wait()
        .unwrap_or_else(|error| panic!("ready wait should succeed: {error}"));
    assert!(waited.entries().is_empty());

    let mut operation = AdminCreateTopics::ready_for_test(Err(KafkaError::new(
        ErrorKind::Backpressure,
        "admin capacity is full",
    )));
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    let Poll::Ready(result) = Pin::new(&mut operation).poll(&mut context) else {
        panic!("ready rejection must not wait");
    };
    assert_eq!(
        result.err().map(|error| error.kind()),
        Some(ErrorKind::Backpressure)
    );
    let Poll::Ready(repeated) = Pin::new(&mut operation).poll(&mut context) else {
        panic!("re-observation must return a ready state error");
    };
    assert_eq!(
        repeated.err().map(|error| error.kind()),
        Some(ErrorKind::State)
    );
    assert!(format!("{operation:?}").contains("accepted_diagnostic"));
}

#[test]
fn private_admin_operation_is_send_without_runtime_dependencies() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<AdminCreateTopics>();
}
