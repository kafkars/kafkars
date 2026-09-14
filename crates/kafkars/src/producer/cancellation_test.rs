//! Public producer cancellation type and lifecycle scenarios.

use std::{
    future::Future,
    time::{Duration, Instant},
};

use bytes::Bytes;

use super::{CancellationOutcome, Delivery, Producer};
use crate::{
    Client, DeliveryStatus, ErrorKind, KafkaError, ProducerLimits, Record, RecordMetadata,
    silent_broker_test::SilentBroker,
};

#[test]
fn delivery_exposes_runtime_neutral_stage_aware_cancellation() {
    fn assert_future<T: Future<Output = Result<RecordMetadata, KafkaError>>>() {}
    fn assert_send<T: Send>() {}
    fn assert_cancel(_: fn(&mut Delivery) -> Result<CancellationOutcome, KafkaError>) {}

    assert_future::<Delivery>();
    assert_send::<Delivery>();
    assert_cancel(Delivery::cancel);
}

#[test]
fn cancellation_preserves_the_terminal_observer_and_repeated_requests_reach_core() {
    let client = build_client();
    let result = client
        .producer()
        .delivery_timeout(Duration::from_secs(5))
        .build();
    let Ok(producer) = result else {
        panic!("valid producer configuration should build")
    };
    let mut delivery = admit(&producer);

    assert_eq!(
        cancel_with_contention_retry(|| delivery.cancel()),
        CancellationOutcome::CancelledNotSent
    );
    assert_eq!(
        cancel_with_contention_retry(|| delivery.cancel()),
        CancellationOutcome::AlreadyTerminal
    );

    let Err(error) = delivery.wait() else {
        panic!("cancelled delivery must retain its terminal failure")
    };
    assert_eq!(error.kind(), ErrorKind::Cancelled);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}

#[test]
fn cancelling_bounded_immediate_and_waiting_sends_keeps_admission_open() {
    let broker = SilentBroker::start();
    let client = Client::builder()
        .bootstrap_servers([broker.endpoint()])
        .producer_limits(
            ProducerLimits::new(1_048_576, 1, 1, 4_096, 1, 262_144, Duration::ZERO)
                .with_request_bytes(524_288)
                .with_max_in_flight_requests_per_broker(1),
        )
        .build()
        .unwrap_or_else(|error| panic!("bounded client should build: {error}"));
    broker.wait_negotiated();
    let producer = client
        .producer()
        .delivery_timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_else(|error| panic!("producer should build: {error}"));

    let mut immediate = admit(&producer);
    assert_cancels_twice(|| immediate.cancel());
    let _terminal = immediate.wait();

    let mut waiting = producer.send(Record::to("orders").partition(0).value("cancel-waiting"));
    assert_cancels_twice(|| waiting.cancel());
    let _terminal = waiting.wait();

    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        match producer.flush().wait() {
            Ok(()) => break,
            Err(error) if error.kind() == ErrorKind::Backpressure => {
                assert!(Instant::now() < deadline, "flush should regain admission");
                std::hint::spin_loop();
            }
            Err(error) => panic!("flush cancelled sends: {error}"),
        }
    }
}

fn assert_cancels_twice(mut cancel: impl FnMut() -> Result<CancellationOutcome, KafkaError>) {
    assert!(matches!(
        cancel_with_contention_retry(&mut cancel),
        CancellationOutcome::CancelledNotSent | CancellationOutcome::TooLate
    ));
    assert_eq!(
        cancel_with_contention_retry(&mut cancel),
        CancellationOutcome::AlreadyTerminal
    );
}

fn cancel_with_contention_retry(
    mut cancel: impl FnMut() -> Result<CancellationOutcome, KafkaError>,
) -> CancellationOutcome {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        match cancel() {
            Ok(outcome) => return outcome,
            Err(error) if error.kind() == ErrorKind::Backpressure => {
                assert!(
                    Instant::now() < deadline,
                    "one cancellation should acquire bounded shard ownership"
                );
                std::hint::spin_loop();
            }
            Err(error) => panic!("valid cancellation failed: {error}"),
        }
    }
}

fn admit(producer: &Producer) -> Delivery {
    let retained = Bytes::from_static(b"created");
    let mut record = Record::to("orders").partition(0).value(retained.clone());
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        match producer.try_send(record) {
            Ok(delivery) => return delivery,
            Err(rejection) if rejection.error().kind() == ErrorKind::Backpressure => {
                assert!(
                    Instant::now() < deadline,
                    "one record should reach bounded admission"
                );
                let (returned, _error) = rejection.into_parts();
                assert_eq!(
                    returned.value_bytes().map(|bytes| bytes.as_ptr()),
                    Some(retained.as_ptr())
                );
                record = returned;
                std::hint::spin_loop();
            }
            Err(rejection) => panic!("valid record admission failed: {}", rejection.error()),
        }
    }
}

fn build_client() -> Client {
    let result = Client::builder().bootstrap_servers(["127.0.0.1:1"]).build();
    let Ok(client) = result else {
        panic!("valid local client configuration should build")
    };
    client
}
