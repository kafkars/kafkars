//! Local pre-admission terminal observation tests.
#![expect(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "the test constructs a known representable past deadline and asserts failure"
)]

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};

use crate::{DeliveryStatus, ErrorKind};

use super::{AdminDeleteAcls, DeleteAclsAdminRequest};

#[test]
fn elapsed_submission_boundary_is_a_locally_ready_unsent_timeout() {
    let operation = AdminDeleteAcls::deadline_elapsed();
    let error = operation.wait().expect_err("elapsed deadline must fail");

    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}

#[test]
fn captured_deadline_expires_before_engine_admission() {
    let operation = AdminDeleteAcls::submit_with(
        DeleteAclsAdminRequest::new(Vec::new()),
        Instant::now()
            .checked_sub(Duration::from_millis(1))
            .unwrap(),
        |_request, _remaining| panic!("expired work must not reach engine admission"),
    );
    let error = operation.wait().expect_err("expired deadline must fail");

    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}

#[test]
fn locally_ready_operation_has_one_future_observation() {
    let mut operation = AdminDeleteAcls::invalid_deadline();
    let waker = Waker::from(std::sync::Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);

    let first = Pin::new(&mut operation).poll(&mut context);
    let second = Pin::new(&mut operation).poll(&mut context);

    assert!(
        matches!(first, Poll::Ready(Err(ref error)) if error.kind() == ErrorKind::Configuration)
    );
    assert!(matches!(second, Poll::Ready(Err(ref error)) if error.kind() == ErrorKind::State));
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: std::sync::Arc<Self>) {}
}
