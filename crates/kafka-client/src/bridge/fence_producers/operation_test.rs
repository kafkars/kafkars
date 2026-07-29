//! Local and ready producer-fencing observation tests.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};

use crate::{BatchResult, DeliveryStatus, ErrorKind, admin::FenceProducersResult};

use super::{operation::AdminFenceProducers, request::FenceProducersAdminRequest};

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

#[test]
fn elapsed_submission_boundary_is_a_locally_ready_unsent_timeout() {
    let error = AdminFenceProducers::deadline_elapsed()
        .wait()
        .expect_err("elapsed deadline must fail");
    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}

#[test]
fn captured_submission_deadline_is_rechecked_after_public_result_preparation() {
    let operation = AdminFenceProducers::submit_with(
        FenceProducersAdminRequest::new(vec!["orders-tx".to_owned()]),
        Instant::now() - Duration::from_millis(1),
        |_request, _remaining| panic!("expired request must not reach engine admission"),
    );
    let error = operation
        .wait()
        .expect_err("elapsed anchored deadline must fail locally");
    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}

#[test]
fn wait_and_future_share_one_linear_result_shape() {
    let ready = FenceProducersResult::new(Duration::ZERO, BatchResult::new(Vec::new()));
    let waited = AdminFenceProducers::ready_for_test(Ok(ready))
        .wait()
        .unwrap_or_else(|error| panic!("ready wait should succeed: {error}"));
    assert!(waited.entries().is_empty());

    let mut operation = AdminFenceProducers::invalid_deadline();
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut operation).poll(&mut context),
        Poll::Ready(Err(error)) if error.kind() == ErrorKind::Configuration
    ));
    assert!(matches!(
        Pin::new(&mut operation).poll(&mut context),
        Poll::Ready(Err(error)) if error.kind() == ErrorKind::State
    ));
}

#[test]
fn private_operation_is_a_send_future_without_a_runtime() {
    fn assert_future<T: Future + Send>() {}
    assert_future::<AdminFenceProducers>();
}
