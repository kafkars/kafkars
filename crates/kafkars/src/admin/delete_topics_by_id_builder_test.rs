//! Topic-ID `DeleteTopics` builder boundary tests.
#![expect(
    clippy::expect_used,
    reason = "the test asserts a topic-ID validation failure"
)]

use std::time::Duration;

use crate::{Client, DeliveryStatus, ErrorKind};

#[test]
fn zero_deadline_fails_definitely_unsent() {
    let result = Client::builder()
        .bootstrap_servers(["127.0.0.1:9092"])
        .build()
        .unwrap_or_else(|error| panic!("build test client: {error}"))
        .admin()
        .delete_topics_by_id([[1; 16]])
        .deadline_after(Duration::ZERO)
        .submit()
        .wait();
    let error = result.expect_err("zero deadline must fail");
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}
