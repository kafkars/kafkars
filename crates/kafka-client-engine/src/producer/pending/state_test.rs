//! Closed phase behavior visible through the pending-cell boundary.

use std::task::Poll;

use super::{
    PendingSendCell, ProducerSendFailure, ProducerSendFailureKind,
    test_support::{CountingWake, poll_send},
};
use crate::{ProducerSendError, producer::boundary::ProducerSend};

#[test]
fn consumed_local_transition_rejects_a_second_observation() {
    let cell = PendingSendCell::new();
    let mut send = ProducerSend::from_pending(cell.clone());
    let wake = CountingWake::new();
    assert_eq!(poll_send(&mut send, wake.clone()), Poll::Pending);
    let promotion = cell
        .begin_promotion()
        .unwrap_or_else(|error| panic!("pending cell should claim: {error:?}"));
    let failure = ProducerSendFailure::new(ProducerSendFailureKind::Cancelled);
    let job = promotion
        .settle_local(failure)
        .unwrap_or_else(|error| panic!("local settlement should commit: {error:?}"));
    job.dispatch();
    assert!(wake.wait_for_wake().is_some());
    assert_eq!(
        poll_send(&mut send, wake.clone()),
        Poll::Ready(Err(ProducerSendError::Local(failure)))
    );
    assert!(matches!(
        poll_send(&mut send, wake),
        Poll::Ready(Err(ProducerSendError::Observer(_)))
    ));
}
