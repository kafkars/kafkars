//! Inert builder ownership scenarios.
#![expect(
    clippy::expect_used,
    reason = "the test asserts local deadline rejection"
)]

use std::time::Duration;

use super::{CreateTopicsBuilder, NewTopic, TopicReplicaAssignment};
use crate::{Client, DeliveryStatus, ErrorKind};

#[test]
fn create_topics_builder_is_send_before_single_submission() {
    fn assert_send<T: Send>() {}
    assert_send::<CreateTopicsBuilder>();
}

#[test]
fn malformed_or_mixed_manual_placement_rejects_definitely_unsent() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));
    let invalid = [
        NewTopic::with_replica_assignments("empty", []),
        NewTopic::with_replica_assignments("non-contiguous", [TopicReplicaAssignment::new(1, [3])]),
        NewTopic::with_replica_assignments(
            "duplicate-broker",
            [TopicReplicaAssignment::new(0, [3, 3])],
        ),
        NewTopic::with_replica_assignments("mixed", [TopicReplicaAssignment::new(0, [3])])
            .replication_factor(2),
    ];

    for topic in invalid {
        let error = client
            .admin()
            .create_topics([topic])
            .submit()
            .wait()
            .expect_err("invalid manual placement must reject before transport");
        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
    }
}

#[test]
fn empty_zero_deadline_builder_is_inert_until_submit() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));
    let builder = client
        .admin()
        .create_topics(std::iter::empty::<NewTopic>())
        .deadline_after(Duration::ZERO);

    let error = builder
        .submit()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("zero deadline must reject at submit"));
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}
