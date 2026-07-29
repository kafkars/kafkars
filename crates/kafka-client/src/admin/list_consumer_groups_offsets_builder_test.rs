//! Public multi-consumer-group builder boundary tests.

use std::time::Duration;

use crate::{Client, DeliveryStatus, ErrorKind};

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
