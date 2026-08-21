//! Compile-time shape tests for the multi-group `StreamsGroup` builder.

use std::time::Duration;

use super::{DescribeStreamsGroups, DescribeStreamsGroupsBuilder};
use crate::{Client, DeliveryStatus, ErrorKind};

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn builder_is_thread_safe_and_has_the_expected_fluent_shape() {
    assert_send_sync::<DescribeStreamsGroupsBuilder>();

    let authorized_operations: fn(
        DescribeStreamsGroupsBuilder,
        bool,
    ) -> DescribeStreamsGroupsBuilder = DescribeStreamsGroupsBuilder::include_authorized_operations;
    let topology_description: fn(
        DescribeStreamsGroupsBuilder,
        bool,
    ) -> DescribeStreamsGroupsBuilder = DescribeStreamsGroupsBuilder::include_topology_description;
    let deadline: fn(DescribeStreamsGroupsBuilder, Duration) -> DescribeStreamsGroupsBuilder =
        DescribeStreamsGroupsBuilder::deadline_after;
    let submit: fn(DescribeStreamsGroupsBuilder) -> DescribeStreamsGroups =
        DescribeStreamsGroupsBuilder::submit;
    let _ = (
        authorized_operations,
        topology_description,
        deadline,
        submit,
    );
}

#[test]
fn public_handle_keeps_zero_deadline_inert_until_submit() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));
    let error = client
        .admin()
        .describe_streams_groups(["streams-a", "streams-b"])
        .deadline_after(Duration::ZERO)
        .submit()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("zero deadline must reject at submit"));

    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}
