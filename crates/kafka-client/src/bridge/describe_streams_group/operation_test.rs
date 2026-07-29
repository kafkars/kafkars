//! Ready and runtime-neutral StreamsGroup description observation tests.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use crate::{
    ErrorKind, KafkaError,
    admin::{DescribeStreamsGroupResult, StreamsGroupDescription},
};

use super::operation::AdminDescribeStreamsGroup;

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

#[test]
fn wait_and_future_share_one_concrete_result_shape() {
    let description = StreamsGroupDescription::new(
        "streams-workers".to_owned(),
        "Empty".to_owned(),
        1,
        1,
        None,
        Vec::new(),
        None,
        None,
        None,
    );
    let result = DescribeStreamsGroupResult::new(Duration::ZERO, description);
    let waited = AdminDescribeStreamsGroup::ready_for_test(Ok(result))
        .wait()
        .unwrap_or_else(|error| panic!("ready wait should succeed: {error}"));
    assert_eq!(waited.description().group_id(), "streams-workers");

    let mut operation = AdminDescribeStreamsGroup::ready_for_test(Err(KafkaError::new(
        ErrorKind::Backpressure,
        "StreamsGroup description capacity is full",
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
    assert_future::<AdminDescribeStreamsGroup>();
}
