//! `StreamsGroup` description builder surface and submit-boundary tests.

use std::{future::Future, time::Duration};

use crate::{Client, DeliveryStatus, ErrorKind};

use super::{DescribeStreamsGroup, DescribeStreamsGroupBuilder, DescribeStreamsGroupResult};

fn assert_future<T: Future<Output = Result<DescribeStreamsGroupResult, crate::KafkaError>>>() {}

#[test]
fn operation_and_inert_builder_surface_are_stable() {
    assert_future::<DescribeStreamsGroup>();
    let authorized: fn(DescribeStreamsGroupBuilder, bool) -> DescribeStreamsGroupBuilder =
        DescribeStreamsGroupBuilder::include_authorized_operations;
    let topology: fn(DescribeStreamsGroupBuilder, bool) -> DescribeStreamsGroupBuilder =
        DescribeStreamsGroupBuilder::include_topology_description;
    let deadline: fn(DescribeStreamsGroupBuilder, Duration) -> DescribeStreamsGroupBuilder =
        DescribeStreamsGroupBuilder::deadline_after;
    let submit: fn(DescribeStreamsGroupBuilder) -> DescribeStreamsGroup =
        DescribeStreamsGroupBuilder::submit;
    let _ = (authorized, topology, deadline, submit);
}

#[test]
fn invalid_group_and_zero_deadline_reject_definitely_unsent_at_submit() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));

    for builder in [
        client.admin().describe_streams_group(""),
        client
            .admin()
            .describe_streams_group("streams-workers")
            .deadline_after(Duration::ZERO),
    ] {
        let error = builder
            .submit()
            .wait()
            .err()
            .unwrap_or_else(|| panic!("invalid StreamsGroup description must reject"));
        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
    }
}
