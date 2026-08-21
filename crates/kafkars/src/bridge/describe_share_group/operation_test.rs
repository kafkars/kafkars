//! Ready and runtime-neutral `ShareGroup` description observation tests.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use crate::{
    ErrorKind, KafkaError,
    admin::{DescribeShareGroupResult, ShareGroupDescription},
};

use super::operation::AdminDescribeShareGroup;

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

#[test]
fn wait_and_future_share_one_concrete_result_shape() {
    let description = ShareGroupDescription::new(
        "share-workers".to_owned(),
        "Empty".to_owned(),
        1,
        1,
        "uniform".to_owned(),
        Vec::new(),
        None,
    );
    let result = DescribeShareGroupResult::new(Duration::ZERO, description);
    let waited = AdminDescribeShareGroup::ready_for_test(Ok(result))
        .wait()
        .unwrap_or_else(|error| panic!("ready wait should succeed: {error}"));
    assert_eq!(waited.description().group_id(), "share-workers");

    let mut operation = AdminDescribeShareGroup::ready_for_test(Err(KafkaError::new(
        ErrorKind::Backpressure,
        "ShareGroup description capacity is full",
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
    assert_future::<AdminDescribeShareGroup>();
}
