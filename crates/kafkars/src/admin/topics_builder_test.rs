//! Inert `DescribeTopics` builder ownership scenarios.

use std::time::Duration;

use super::DescribeTopicsBuilder;
use crate::{Client, DeliveryStatus, ErrorKind};

#[test]
fn describe_topics_builder_is_send_before_single_submission() {
    fn assert_send<T: Send>() {}
    assert_send::<DescribeTopicsBuilder>();
    let option: fn(DescribeTopicsBuilder, bool) -> DescribeTopicsBuilder =
        DescribeTopicsBuilder::include_authorized_operations;
    let _ = option;
}

#[test]
fn empty_zero_deadline_builder_is_inert_until_submit() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));
    let builder = client
        .admin()
        .describe_topics(std::iter::empty::<String>())
        .deadline_after(Duration::ZERO);
    let error = builder
        .submit()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("zero deadline must reject at submit"));
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}
