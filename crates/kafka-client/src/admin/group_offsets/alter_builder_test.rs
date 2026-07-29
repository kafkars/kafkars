//! Inert group-offset alteration ownership and rejection scenarios.

use std::time::Duration;

use crate::{
    AlterConsumerGroupOffsetsBuilder, Client, ConsumerGroupOffsetAlteration, DeliveryStatus,
    ErrorKind,
};

#[test]
fn builder_is_send_before_single_submission() {
    fn assert_send<T: Send>() {}
    assert_send::<AlterConsumerGroupOffsetsBuilder>();
}

#[test]
fn duplicate_targets_are_rejected_definitely_unsent_only_at_submit() {
    let error = client()
        .admin()
        .alter_consumer_group_offsets(
            "payments",
            [
                ConsumerGroupOffsetAlteration::new("orders", 7, 42),
                ConsumerGroupOffsetAlteration::new("orders", 7, 43),
            ],
        )
        .submit()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("duplicate targets must reject at submit"));
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}

#[test]
fn zero_deadline_is_rejected_at_the_public_submission_boundary() {
    let error = client()
        .admin()
        .alter_consumer_group_offsets(
            "payments",
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

#[test]
fn oversized_retention_time_remains_inert_until_submit() {
    let error = client()
        .admin()
        .alter_consumer_group_offsets(
            "payments",
            [ConsumerGroupOffsetAlteration::new("orders", 7, 42)],
        )
        .retention_time(Duration::MAX)
        .submit()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("oversized retention time must reject at submit"));
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}

#[test]
fn retention_time_and_leader_epoch_reject_definitely_unsent_at_submit() {
    let error = client()
        .admin()
        .alter_consumer_group_offsets(
            "payments",
            [ConsumerGroupOffsetAlteration::new("orders", 7, 42).leader_epoch(9)],
        )
        .retention_time(Duration::from_secs(30))
        .submit()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("retention time with leader epoch must reject at submit"));
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}

fn client() -> Client {
    Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"))
}
