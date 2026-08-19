//! Public topic-ID builder contract tests.
#![expect(
    clippy::expect_used,
    reason = "the test asserts a topic-ID validation failure"
)]

use std::time::Duration;

use crate::DescribeTopicsByIdBuilder;
use crate::{Client, ErrorKind};

#[test]
fn authorized_operations_option_is_additive_and_inert() {
    let option: fn(DescribeTopicsByIdBuilder, bool) -> DescribeTopicsByIdBuilder =
        DescribeTopicsByIdBuilder::include_authorized_operations;
    let _ = option;
}

#[test]
fn zero_deadline_fails_at_submission_without_driver_ownership() {
    let result = Client::builder()
        .bootstrap_servers(["127.0.0.1:9092"])
        .build()
        .unwrap_or_else(|error| panic!("build test client: {error}"))
        .admin()
        .describe_topics_by_id([[1; 16]])
        .deadline_after(Duration::ZERO)
        .submit()
        .wait();
    let error = result.expect_err("zero deadline must fail");
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(
        error.delivery_status(),
        Some(crate::DeliveryStatus::NotSent)
    );
}
