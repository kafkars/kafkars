//! Inert group-offset builder ownership and deadline scenarios.
#![expect(
    clippy::expect_used,
    reason = "the test asserts local deadline rejection"
)]

use std::time::Duration;

use super::ListConsumerGroupOffsetsBuilder;
use crate::{Client, DeliveryStatus, ErrorKind, StartPosition, TopicPartition};

#[test]
fn builder_is_send_before_single_submission() {
    fn assert_send<T: Send>() {}
    assert_send::<ListConsumerGroupOffsetsBuilder>();
}

#[test]
fn assignment_position_is_retained_until_submit_and_rejected_not_sent() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));
    let error = client
        .admin()
        .list_consumer_group_offsets("payments")
        .partitions([
            TopicPartition::new("orders", 3),
            TopicPartition::new("audit", 1).start_at(StartPosition::Beginning),
        ])
        .submit()
        .wait()
        .expect_err("assignment-only start position must reject at submit");
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}

#[test]
fn zero_deadline_and_stability_option_remain_inert_until_submit() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));
    let error = client
        .admin()
        .list_consumer_group_offsets("payments")
        .require_stable(true)
        .deadline_after(Duration::ZERO)
        .submit()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("zero deadline must reject at submit"));
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}
