//! Public ergonomic send shape and ready-error scenarios.

use std::future::Future;

use crate::{DeliveryStatus, ErrorKind, KafkaError, Record, RecordMetadata};

use super::Send;

#[test]
fn named_send_exposes_one_async_and_blocking_result_type() {
    fn assert_future<T: Future<Output = Result<RecordMetadata, KafkaError>>>() {}
    fn assert_wait(_: fn(Send) -> Result<RecordMetadata, KafkaError>) {}

    assert_future::<Send>();
    assert_wait(Send::wait);
}

#[test]
fn invalid_record_is_ready_with_not_sent_certainty() {
    let client = crate::Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("local client construction should succeed: {error}"));
    let producer = client
        .producer()
        .build()
        .unwrap_or_else(|error| panic!("local producer construction should succeed: {error}"));

    let error = producer
        .send(Record::to("").value("created"))
        .wait()
        .err()
        .unwrap_or_else(|| panic!("an empty topic must fail local record validation"));

    assert_eq!(error.kind(), ErrorKind::InvalidRecord);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}
