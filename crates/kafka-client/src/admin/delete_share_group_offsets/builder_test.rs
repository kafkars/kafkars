//! ShareGroup offset-deletion builder surface tests.

use std::{future::Future, time::Duration};

use crate::{Client, DeliveryStatus, ErrorKind};

use super::{
    DeleteShareGroupOffsets, DeleteShareGroupOffsetsBuilder, DeleteShareGroupOffsetsResult,
};

fn assert_future<T: Future<Output = Result<DeleteShareGroupOffsetsResult, crate::KafkaError>>>() {}

#[test]
fn operation_is_a_named_runtime_neutral_future() {
    assert_future::<DeleteShareGroupOffsets>();
}

#[test]
fn builder_keeps_timeout_and_submission_configuration_inert() {
    let deadline: fn(DeleteShareGroupOffsetsBuilder, Duration) -> DeleteShareGroupOffsetsBuilder =
        DeleteShareGroupOffsetsBuilder::deadline_after;
    let submit: fn(DeleteShareGroupOffsetsBuilder) -> DeleteShareGroupOffsets =
        DeleteShareGroupOffsetsBuilder::submit;

    let _ = (deadline, submit);
}

#[test]
fn invalid_group_and_topic_shapes_reject_definitely_unsent_at_submit() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));

    for (group_id, topics) in [
        ("", vec!["orders"]),
        ("workers", Vec::new()),
        ("workers", vec![""]),
        ("workers", vec!["orders", "orders"]),
    ] {
        let error = client
            .admin()
            .delete_share_group_offsets(group_id, topics)
            .deadline_after(Duration::from_secs(1))
            .submit()
            .wait()
            .err()
            .unwrap_or_else(|| panic!("invalid ShareGroup deletion must reject"));
        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
    }
}

#[test]
fn zero_deadline_is_deferred_to_the_submit_boundary() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));
    let error = client
        .admin()
        .delete_share_group_offsets("workers", ["orders"])
        .deadline_after(Duration::ZERO)
        .submit()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("zero deadline must reject"));

    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}
