//! ShareGroup offset-alteration builder surface tests.

use std::{future::Future, time::Duration};

use crate::{Client, DeliveryStatus, ErrorKind};

use super::{
    AlterShareGroupOffsets, AlterShareGroupOffsetsBuilder, AlterShareGroupOffsetsResult,
    ShareGroupOffsetAlteration,
};

fn assert_future<T: Future<Output = Result<AlterShareGroupOffsetsResult, crate::KafkaError>>>() {}

#[test]
fn operation_is_a_named_runtime_neutral_future() {
    assert_future::<AlterShareGroupOffsets>();
}

#[test]
fn builder_keeps_timeout_and_submission_configuration_inert() {
    let deadline: fn(AlterShareGroupOffsetsBuilder, Duration) -> AlterShareGroupOffsetsBuilder =
        AlterShareGroupOffsetsBuilder::deadline_after;
    let submit: fn(AlterShareGroupOffsetsBuilder) -> AlterShareGroupOffsets =
        AlterShareGroupOffsetsBuilder::submit;

    let _ = (deadline, submit);
}

#[test]
fn invalid_and_duplicate_targets_reject_definitely_unsent_at_submit() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));

    for (group_id, alterations) in [
        ("", vec![ShareGroupOffsetAlteration::new("orders", 0, 3)]),
        ("workers", Vec::new()),
        ("workers", vec![ShareGroupOffsetAlteration::new("", 0, 3)]),
        (
            "workers",
            vec![ShareGroupOffsetAlteration::new("orders", -1, 3)],
        ),
        (
            "workers",
            vec![ShareGroupOffsetAlteration::new("orders", 0, -1)],
        ),
        (
            "workers",
            vec![
                ShareGroupOffsetAlteration::new("orders", 0, 3),
                ShareGroupOffsetAlteration::new("orders", 0, 4),
            ],
        ),
    ] {
        let error = client
            .admin()
            .alter_share_group_offsets(group_id, alterations)
            .deadline_after(Duration::from_secs(1))
            .submit()
            .wait()
            .err()
            .unwrap_or_else(|| panic!("invalid ShareGroup alteration must reject"));
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
        .alter_share_group_offsets("workers", [ShareGroupOffsetAlteration::new("orders", 0, 3)])
        .deadline_after(Duration::ZERO)
        .submit()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("zero deadline must reject"));

    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}
