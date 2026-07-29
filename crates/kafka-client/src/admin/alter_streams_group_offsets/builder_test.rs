//! Typed StreamsGroup builder delegation and deadline-boundary tests.

use std::time::Duration;

use super::{AlterStreamsGroupOffsets, AlterStreamsGroupOffsetsBuilder};
use crate::{Client, ConsumerGroupOffsetAlteration, DeliveryStatus, ErrorKind};

#[test]
fn builder_is_send_and_keeps_the_named_submission_surface() {
    fn assert_send<T: Send>() {}
    assert_send::<AlterStreamsGroupOffsetsBuilder>();

    let deadline: fn(AlterStreamsGroupOffsetsBuilder, Duration) -> AlterStreamsGroupOffsetsBuilder =
        AlterStreamsGroupOffsetsBuilder::deadline_after;
    let submit: fn(AlterStreamsGroupOffsetsBuilder) -> AlterStreamsGroupOffsets =
        AlterStreamsGroupOffsetsBuilder::submit;
    let _ = (deadline, submit);
}

#[test]
fn debug_is_typed_and_preserves_the_existing_redacted_request_diagnostics() {
    let builder = client().admin().alter_streams_group_offsets(
        "streams-workers",
        [ConsumerGroupOffsetAlteration::new("orders", 7, 42)],
    );
    let debug = format!("{builder:?}");

    assert!(debug.contains("AlterStreamsGroupOffsetsBuilder"));
    assert!(debug.contains("AlterConsumerGroupOffsetsBuilder"));
    assert!(debug.contains("AlterConsumerGroupOffsetsAdminRequest"));
    assert!(!debug.contains("streams-workers"));
    assert!(!debug.contains("orders"));
}

#[test]
fn zero_deadline_stays_inert_until_the_underlying_submit_boundary() {
    let error = client()
        .admin()
        .alter_streams_group_offsets(
            "streams-workers",
            [ConsumerGroupOffsetAlteration::new("orders", 7, 42)],
        )
        .deadline_after(Duration::ZERO)
        .submit()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("zero deadline must reject at submit"));

    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}

fn client() -> Client {
    Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"))
}
