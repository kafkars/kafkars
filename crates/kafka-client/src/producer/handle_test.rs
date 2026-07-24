//! Public producer scenarios for accepted delivery and exact rejection.

use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::{
    Client, Delivery, DeliveryStatus, ErrorKind, Producer,
    record::{Header, Record, RecordParts},
    silent_broker_test::SilentBroker,
};

#[test]
fn accepted_record_returns_public_delivery_with_one_end_to_end_timeout() {
    let broker = SilentBroker::start();
    let client = build_client_at(broker.endpoint());
    broker.wait_negotiated();
    let producer = client
        .producer()
        .delivery_timeout(Duration::from_millis(200))
        .build();
    let Ok(producer) = producer else {
        panic!("nonzero producer timeout should build")
    };
    let retained = Bytes::from_static(b"created");
    let record = Record::to("orders")
        .partition(0)
        .key("order-42")
        .value(retained.clone());

    let delivery = admit_with_backpressure_retry(&producer, record, &retained);
    let Err(error) = delivery.wait() else {
        panic!("a silent broker cannot initialize the producer before its deadline")
    };

    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}

#[test]
fn empty_flush_completes_through_the_public_named_operation() {
    let result = build_client().producer().build();
    let Ok(producer) = result else {
        panic!("producer construction should remain local")
    };

    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        match producer.flush().wait() {
            Ok(()) => break,
            Err(error) if error.kind() == ErrorKind::Backpressure => {
                assert!(
                    Instant::now() < deadline,
                    "one empty flush should be admitted after startup contention"
                );
                std::hint::spin_loop();
            }
            Err(error) => panic!("empty flush should succeed: {error}"),
        }
    }
}

#[test]
fn zero_delivery_timeout_is_rejected_when_the_producer_builds() {
    let result = build_client()
        .producer()
        .delivery_timeout(Duration::ZERO)
        .build();
    let Err(error) = result else {
        panic!("zero producer timeout must fail local validation")
    };

    assert_eq!(error.kind(), ErrorKind::Configuration);
}

#[test]
fn unrepresentable_delivery_timeout_is_rejected_when_the_producer_builds() {
    let result = build_client()
        .producer()
        .delivery_timeout(Duration::MAX)
        .build();
    let Err(error) = result else {
        panic!("unrepresentable producer timeout must fail local validation")
    };

    assert_eq!(error.kind(), ErrorKind::Configuration);
}

#[test]
fn try_send_rejection_returns_exact_nullable_ordered_bytes_storage() {
    let client = build_client();
    let result = client.producer().build();
    let Ok(producer) = result else {
        panic!("producer construction should remain local")
    };
    let retained = Bytes::from(vec![9, 8, 7]);
    let record = Record::from_parts(RecordParts {
        topic: "orders".to_owned(),
        partition: None,
        timestamp_milliseconds: Some(42),
        key: None,
        value: Some(Bytes::new()),
        headers: vec![
            Header::from_parts("trace".to_owned(), None),
            Header::from_parts("trace".to_owned(), Some(Bytes::new())),
            Header::from_parts("trace".to_owned(), Some(retained.clone())),
        ],
    });

    let Err(rejection) = producer.try_send(record) else {
        panic!("automatic partitioning must remain unavailable")
    };
    assert_eq!(rejection.error().kind(), ErrorKind::InvalidRecord);
    assert_eq!(
        rejection.error().delivery_status(),
        Some(DeliveryStatus::NotSent)
    );
    let (returned, error) = rejection.into_parts();

    assert_eq!(error.kind(), ErrorKind::InvalidRecord);
    assert_eq!(returned.topic(), "orders");
    assert_eq!(returned.explicit_partition(), None);
    assert_eq!(returned.timestamp(), Some(42));
    assert_eq!(returned.key_bytes(), None);
    assert_eq!(returned.value_bytes(), Some(&Bytes::new()));
    assert_eq!(returned.headers()[0].value(), None);
    assert_eq!(returned.headers()[1].value(), Some(&Bytes::new()));
    assert_eq!(
        returned.headers()[2].value().map(|bytes| bytes.as_ptr()),
        Some(retained.as_ptr())
    );
}

fn admit_with_backpressure_retry(
    producer: &Producer,
    mut record: Record,
    retained: &Bytes,
) -> Delivery {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        match producer.try_send(record) {
            Ok(delivery) => return delivery,
            Err(rejection) if rejection.error().kind() == ErrorKind::Backpressure => {
                assert!(
                    Instant::now() < deadline,
                    "one explicit-partition record should reach bounded admission"
                );
                let (returned, _error) = rejection.into_parts();
                assert_eq!(
                    returned.value_bytes().map(|bytes| bytes.as_ptr()),
                    Some(retained.as_ptr()),
                    "bounded retry must retain the exact record storage"
                );
                record = returned;
                std::hint::spin_loop();
            }
            Err(rejection) => panic!("valid record admission failed: {}", rejection.error()),
        }
    }
}

fn build_client() -> Client {
    build_client_at("127.0.0.1:1".to_owned())
}

fn build_client_at(endpoint: String) -> Client {
    let result = Client::builder().bootstrap_servers([endpoint]).build();
    let Ok(client) = result else {
        panic!("valid local client configuration should build")
    };
    client
}
