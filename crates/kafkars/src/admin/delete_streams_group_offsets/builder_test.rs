//! Streams-group offset-deletion builder delegation tests.

use std::time::Duration;

use super::{DeleteStreamsGroupOffsets, DeleteStreamsGroupOffsetsBuilder};
use crate::{Client, DeliveryStatus, ErrorKind, TopicPartition};

#[test]
fn builder_is_send_and_exposes_one_submission_boundary() {
    fn assert_send_debug<T: Send + std::fmt::Debug>() {}
    assert_send_debug::<DeleteStreamsGroupOffsetsBuilder>();

    let deadline: fn(
        DeleteStreamsGroupOffsetsBuilder,
        Duration,
    ) -> DeleteStreamsGroupOffsetsBuilder = DeleteStreamsGroupOffsetsBuilder::deadline_after;
    let submit: fn(DeleteStreamsGroupOffsetsBuilder) -> DeleteStreamsGroupOffsets =
        DeleteStreamsGroupOffsetsBuilder::submit;

    let _ = (deadline, submit);
}

#[test]
fn public_handle_keeps_zero_deadline_inert_until_submit() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));
    let error = client
        .admin()
        .delete_streams_group_offsets("streams-workers", [TopicPartition::new("orders", 7)])
        .deadline_after(Duration::ZERO)
        .submit()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("zero deadline must reject at submit"));

    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}
