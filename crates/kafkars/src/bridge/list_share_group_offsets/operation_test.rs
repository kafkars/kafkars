//! Private ready `ShareGroup` offset-listing observation tests.

use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

use crate::{ErrorKind, KafkaError};

use super::operation::AdminListShareGroupOffsets;

#[test]
fn ready_rejection_is_observed_once() {
    let operation = AdminListShareGroupOffsets::ready_for_test(Err(KafkaError::new(
        ErrorKind::Configuration,
        "invalid request",
    )));
    let mut operation = Box::pin(operation);
    let mut context = Context::from_waker(Waker::noop());

    assert!(matches!(
        operation.as_mut().poll(&mut context),
        Poll::Ready(Err(error)) if error.kind() == ErrorKind::Configuration
    ));
    assert!(matches!(
        operation.as_mut().poll(&mut context),
        Poll::Ready(Err(error)) if error.kind() == ErrorKind::State
    ));
}
