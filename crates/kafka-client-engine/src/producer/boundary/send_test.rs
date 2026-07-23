//! Runtime-neutral send construction and direct-state scenarios.

use std::task::Poll;

use kafka_client_core::{
    DeliveryStatus as CoreDeliveryStatus, ProducerCompletion, ProducerFailure,
};

use super::ProducerSend;
use crate::{
    ProducerDeliveryError, ProducerDeliveryObserver, ProducerDeliveryStatus, ProducerSendError,
    ProducerSendFailure, ProducerSendFailureKind,
    completion::{CompletionRegistry, ReclaimStatus},
    producer::pending::test_support::{CountingWake, poll_send},
};

#[test]
fn immediately_ready_local_send_reports_exact_not_sent_vocabulary() {
    for kind in [
        ProducerSendFailureKind::DeadlineElapsed,
        ProducerSendFailureKind::Shutdown,
        ProducerSendFailureKind::Closed,
        ProducerSendFailureKind::Backpressure,
        ProducerSendFailureKind::Cancelled,
    ] {
        let failure = ProducerSendFailure::new(kind);
        let mut send = ProducerSend::from_local_failure(failure);
        assert_eq!(
            poll_send(&mut send, CountingWake::new()),
            Poll::Ready(Err(ProducerSendError::Local(failure)))
        );
        assert_eq!(failure.kind(), kind);
        assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);
    }
}

#[test]
fn blocking_wait_observes_an_immediately_ready_local_send() {
    let failure = ProducerSendFailure::new(ProducerSendFailureKind::Backpressure);
    assert_eq!(
        ProducerSend::from_local_failure(failure).wait(),
        Err(ProducerSendError::Local(failure))
    );
}

#[test]
fn directly_accepted_send_delegates_to_the_existing_delivery_observer() {
    let mut registry = CompletionRegistry::new(1, 1)
        .unwrap_or_else(|error| panic!("completion notifier should start: {error}"));
    let (id, observer) = registry
        .reserve()
        .unwrap_or_else(|error| panic!("completion should reserve: {error}"));
    let send = ProducerSend::from_accepted(ProducerDeliveryObserver::from_completion(observer));
    assert_eq!(
        registry.publish(
            id,
            ProducerCompletion::Failed(ProducerFailure::execution_unavailable(
                CoreDeliveryStatus::NotSent,
            )),
        ),
        Ok(())
    );
    let Err(ProducerSendError::Delivery(ProducerDeliveryError::Failed(failure))) = send.wait()
    else {
        panic!("accepted send should expose the existing delivery failure");
    };
    assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);
    let mut reclaim = None;
    for _attempt in 0..10_000 {
        match registry.next_reclaim() {
            Ok(Some(id)) => {
                reclaim = Some(id);
                break;
            }
            Ok(None) => std::thread::yield_now(),
            Err(error) => panic!("completion reclaim should remain connected: {error}"),
        }
    }
    let reclaim =
        reclaim.unwrap_or_else(|| panic!("accepted completion should become reclaimable"));
    assert_eq!(reclaim, id);
    assert_eq!(registry.finish_reclaim(id), Ok(ReclaimStatus::Reclaimed));
    let join = registry
        .stop_notifier()
        .unwrap_or_else(|error| panic!("completion notifier should stop: {error}"));
    assert_eq!(join.join_off_notifier(), Ok(()));
}
