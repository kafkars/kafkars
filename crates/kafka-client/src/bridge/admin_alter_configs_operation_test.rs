//! Ready and runtime-neutral incremental configuration observation scenarios.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use super::admin_alter_configs_operation::AdminIncrementalAlterConfigs;
use crate::{
    ErrorKind, KafkaError,
    admin::{BatchResult, IncrementalAlterConfigsResult},
};

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

#[test]
fn wait_and_future_share_one_concrete_result_shape() {
    let result = IncrementalAlterConfigsResult::new(Duration::ZERO, BatchResult::new(Vec::new()));
    let waited = AdminIncrementalAlterConfigs::ready_for_test(Ok(result))
        .wait()
        .unwrap_or_else(|error| panic!("ready wait should succeed: {error}"));
    assert!(waited.topics().entries().is_empty());

    let mut operation = AdminIncrementalAlterConfigs::ready_for_test(Err(KafkaError::new(
        ErrorKind::Backpressure,
        "incremental configuration result capacity is full",
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
fn private_incremental_alter_configs_operation_is_send_without_runtime_dependencies() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<AdminIncrementalAlterConfigs>();
}
