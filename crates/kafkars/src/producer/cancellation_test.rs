//! Public producer cancellation type and lifecycle scenarios.

use std::{
    future::Future,
    time::{Duration, Instant},
};

use bytes::Bytes;

use super::{CancellationOutcome, Delivery, Producer};
use crate::{Client, DeliveryStatus, ErrorKind, KafkaError, Record, RecordMetadata};

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
        cancel_with_contention_retry(&mut delivery),
        CancellationOutcome::CancelledNotSent
    );
    assert_eq!(
        cancel_with_contention_retry(&mut delivery),
        CancellationOutcome::AlreadyTerminal
    );

    let Err(error) = delivery.wait() else {
        panic!("cancelled delivery must retain its terminal failure")
    };
    assert_eq!(error.kind(), ErrorKind::Cancelled);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}

fn cancel_with_contention_retry(delivery: &mut Delivery) -> CancellationOutcome {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        match delivery.cancel() {
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
