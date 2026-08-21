//! Public batch admission, exact rejection, and empty-observation scenarios.

use std::{
    thread,
    time::{Duration, Instant},
};

use bytes::Bytes;

use super::Producer;
use crate::{Client, DeliveryStatus, ErrorKind, ProducerLimits, Record};

#[test]
fn empty_batch_is_an_immediately_complete_named_operation() {
    let producer = producer();

    let result = producer.send_batch(Vec::<Record>::new()).wait();

    assert!(result.deliveries().is_empty());
    assert!(result.rejection().is_none());
}

#[test]
fn mixed_partition_modes_reach_the_actual_invalid_record_without_losing_order() {
    let producer = producer();
    let first = Bytes::from(vec![1, 2, 3]);
    let explicit = Bytes::from(vec![10, 11, 12]);
    let invalid = Bytes::from(vec![4, 5, 6]);
    let after = Bytes::from(vec![7, 8, 9]);
    let records = vec![
        Record::to("orders").value(first.clone()),
        Record::to("orders").partition(0).value(explicit.clone()),
        Record::to("orders").partition(-1).value(invalid.clone()),
        Record::to("audit").value(after.clone()),
    ];

    let (deliveries, rejection) = producer.send_batch(records).wait().into_parts();
    let rejection = rejection.unwrap_or_else(|| panic!("negative partition must reject"));
    let (records, error) = rejection.into_parts();

    assert!(deliveries.is_empty());
    assert_eq!(error.kind(), ErrorKind::InvalidRecord);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
    assert_eq!(
        records.iter().map(Record::topic).collect::<Vec<_>>(),
        vec!["orders", "orders", "orders", "audit"]
    );
    assert_eq!(
        records
            .iter()
            .map(Record::explicit_partition)
            .collect::<Vec<_>>(),
        vec![None, Some(0), Some(-1), None]
    );
    assert_eq!(
        records[0].value_bytes().map(|bytes| bytes.as_ptr()),
        Some(first.as_ptr())
    );
    assert_eq!(
        records[1].value_bytes().map(|bytes| bytes.as_ptr()),
        Some(explicit.as_ptr())
    );
    assert_eq!(
        records[2].value_bytes().map(|bytes| bytes.as_ptr()),
        Some(invalid.as_ptr())
    );
    assert_eq!(
        records[3].value_bytes().map(|bytes| bytes.as_ptr()),
        Some(after.as_ptr())
    );
}

#[test]
fn oversized_batch_returns_the_intact_caller_vector_before_conversion() {
    let producer = producer();
    let mut records = Vec::with_capacity(1_025);
    for index in 0_u32..1_025 {
        records.push(
            Record::to("orders")
                .partition(0)
                .value(Bytes::copy_from_slice(&index.to_be_bytes())),
        );
    }
    let allocation = records.as_ptr();

    let (deliveries, rejection) = producer.send_batch(records).wait().into_parts();
    let rejection = rejection.unwrap_or_else(|| panic!("oversized batch must reject"));

    assert!(deliveries.is_empty());
    assert_eq!(rejection.error().kind(), ErrorKind::Backpressure);
    assert_eq!(
        rejection.error().delivery_status(),
        Some(DeliveryStatus::NotSent)
    );
    assert_eq!(rejection.record().len(), 1_025);
    assert_eq!(rejection.record().as_ptr(), allocation);
}

#[test]
fn public_byte_capacity_admits_prefix_and_preserves_exact_suffix() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .producer_limits(ProducerLimits::new(
            256,
            3,
            1,
            256,
            1,
            256,
            Duration::from_millis(1),
        ))
        .build()
        .unwrap_or_else(|error| panic!("bounded client should build: {error}"));
    let producer = client
        .producer()
        .delivery_timeout(Duration::from_secs(1))
        .build()
        .unwrap_or_else(|error| panic!("bounded producer should build: {error}"));
    let rejected = Bytes::from(vec![7; 256]);
    let untouched = Bytes::from_static(b"untouched");

    let result = send_batch_until_admitted(
        &producer,
        vec![
            Record::to("orders").partition(2).value("accepted"),
            Record::to("orders").partition(2).value(rejected.clone()),
            Record::to("audit").partition(1).value(untouched.clone()),
        ],
    );
    let (deliveries, rejection) = result.into_parts();
    let rejection = rejection.unwrap_or_else(|| panic!("byte capacity must reject suffix"));
    let (records, error) = rejection.into_parts();

    assert_eq!(
        deliveries.len(),
        1,
        "unexpected first rejection: {error:?}; returned records: {}",
        records.len()
    );
    assert_eq!(error.kind(), ErrorKind::Backpressure);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
    let [first, second] = records.as_slice() else {
        panic!("rejection must retain first rejected record and untouched suffix")
    };
    assert_eq!(
        (first.topic(), first.explicit_partition()),
        ("orders", Some(2))
    );
    assert_eq!(
        first.value_bytes().map(|bytes| bytes.as_ptr()),
        Some(rejected.as_ptr())
    );
    assert_eq!(
        (second.topic(), second.explicit_partition()),
        ("audit", Some(1))
    );
    assert_eq!(
        second.value_bytes().map(|bytes| bytes.as_ptr()),
        Some(untouched.as_ptr())
    );
}

fn send_batch_until_admitted(
    producer: &Producer,
    mut records: Vec<Record>,
) -> super::SendBatchResult {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let result = producer.send_batch(records).wait();
        let retryable = result.deliveries().is_empty()
            && result.rejection().is_some_and(|rejection| {
                rejection.error().kind() == ErrorKind::Backpressure
                    && rejection.error().delivery_status() == Some(DeliveryStatus::NotSent)
            });
        if !retryable || Instant::now() >= deadline {
            return result;
        }
        let (_deliveries, rejection) = result.into_parts();
        records = rejection
            .unwrap_or_else(|| unreachable!("retryable batch retained its rejection"))
            .into_parts()
            .0;
        thread::sleep(Duration::from_millis(1));
    }
}

fn producer() -> Producer {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("valid local client should build: {error}"));
    client
        .producer()
        .build()
        .unwrap_or_else(|error| panic!("default producer should build: {error}"))
}
