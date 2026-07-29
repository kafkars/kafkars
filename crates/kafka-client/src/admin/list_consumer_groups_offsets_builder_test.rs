//! Public multi-consumer-group builder boundary tests.

use std::time::Duration;

use crate::{
    Client, DeliveryStatus, ErrorKind, StartPosition, TopicPartition,
    admin::ListConsumerGroupOffsetsQuery,
};

#[test]
fn zero_deadline_fails_before_driver_ownership() {
    let result = Client::builder()
        .bootstrap_servers(["127.0.0.1:9092"])
        .build()
        .unwrap_or_else(|error| panic!("build test client: {error}"))
        .admin()
        .list_consumer_groups_offsets(["orders", "audit"])
        .deadline_after(Duration::ZERO)
        .submit()
        .wait();
    let error = result.expect_err("zero deadline must fail");
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}

#[test]
fn selected_assignment_position_rejects_the_complete_plural_operation_not_sent() {
    let result = Client::builder()
        .bootstrap_servers(["127.0.0.1:9092"])
        .build()
        .unwrap_or_else(|error| panic!("build test client: {error}"))
        .admin()
        .list_consumer_groups_offsets([
            ListConsumerGroupOffsetsQuery::all("orders"),
            ListConsumerGroupOffsetsQuery::selected(
                "audit",
                [TopicPartition::new("events", 1).start_at(StartPosition::End)],
            ),
        ])
        .submit()
        .wait();
    let error = result.expect_err("assignment-only start position must fail");
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}
