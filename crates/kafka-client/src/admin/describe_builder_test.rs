//! Inert `DescribeCluster` builder ownership scenarios.

use std::time::Duration;

use super::DescribeClusterBuilder;
use crate::{Client, DeliveryStatus, ErrorKind};

#[test]
fn builder_is_send_before_single_submission() {
    fn assert_send<T: Send>() {}
    assert_send::<DescribeClusterBuilder>();
    let _authorized_operations: fn(DescribeClusterBuilder, bool) -> DescribeClusterBuilder =
        DescribeClusterBuilder::include_authorized_operations;
    let _fenced_brokers: fn(DescribeClusterBuilder, bool) -> DescribeClusterBuilder =
        DescribeClusterBuilder::include_fenced_brokers;
    let _ = (_authorized_operations, _fenced_brokers);
}

#[test]
fn zero_deadline_builder_is_inert_until_submit() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));
    let builder = client
        .admin()
        .describe_cluster()
        .deadline_after(Duration::ZERO);
    let error = builder
        .submit()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("zero deadline must reject at submit"));
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}
