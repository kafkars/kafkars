//! Critical-section evidence for cancellation during producer route lookup.

use kafka_client_core::Moment;

use crate::{
    ProducerCancellationOutcome, ProducerDeliveryError, ProducerDeliveryFailureKind,
    ProducerDeliveryStatus, driver::TrackedProduceCalls,
};

use super::super::produce_test::{driver, prepared_producer, shutdown};
use super::{admit_one, apply_routing_ready};

#[test]
fn accepted_route_lookup_keeps_cancellation_live_before_broker_admission() {
    let (producer, observer) = prepared_producer();
    let mut driver = driver();
    let mut calls = TrackedProduceCalls::new(1);
    let mut routing = None;
    {
        let mut data = producer
            .try_data()
            .unwrap_or_else(|error| panic!("lock producer shard: {error:?}"));
        let outcome = admit_one(
            &driver,
            &mut calls,
            &mut routing,
            &mut data,
            Moment::from_tick(2),
            64,
        )
        .unwrap_or_else(|error| panic!("tracked Produce admission: {error}"));
        assert!(outcome.did_progress());
        assert_eq!(outcome.prepared_batches(), 0);
        assert!(routing.is_some());
        assert_eq!(calls.retained_count(), 0);
        assert!(calls.try_reserve().is_some());
    }

    let cancellation = observer
        .try_cancel()
        .unwrap_or_else(|error| panic!("submitted cancellation decision: {error}"));
    assert_eq!(
        cancellation.outcome(),
        ProducerCancellationOutcome::CancelledNotSent
    );
    {
        let mut data = producer
            .try_data()
            .unwrap_or_else(|error| panic!("lock cancelled producer shard: {error:?}"));
        let stats = data.shard_stats().host;
        assert_eq!(stats.prepared_batches, 0);
        assert_eq!(stats.prepared_bytes, 0);
        assert_eq!(stats.submission_deadlines, 0);
        assert!(
            apply_routing_ready(&mut routing, &mut data, Moment::from_tick(3), 64)
                .unwrap_or_else(|error| panic!("discard stale route lookup: {error}"))
                .did_progress()
        );
        assert!(routing.is_none());
    }
    let Err(ProducerDeliveryError::Failed(failure)) = observer.wait() else {
        panic!("cancelled route lookup must publish one failure")
    };
    assert_eq!(failure.kind(), ProducerDeliveryFailureKind::Cancelled);
    assert_eq!(failure.delivery_status(), ProducerDeliveryStatus::NotSent);

    drop(calls);
    shutdown(&mut driver);
}
