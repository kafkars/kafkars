//! `ShareGroup` description builder surface and submit-boundary tests.

use std::{future::Future, time::Duration};

use crate::{Client, DeliveryStatus, ErrorKind};

use super::{DescribeShareGroup, DescribeShareGroupBuilder, DescribeShareGroupResult};

fn assert_future<T: Future<Output = Result<DescribeShareGroupResult, crate::KafkaError>>>() {}

#[test]
fn operation_and_inert_builder_surface_are_stable() {
    assert_future::<DescribeShareGroup>();
    let authorized: fn(DescribeShareGroupBuilder, bool) -> DescribeShareGroupBuilder =
        DescribeShareGroupBuilder::include_authorized_operations;
    let deadline: fn(DescribeShareGroupBuilder, Duration) -> DescribeShareGroupBuilder =
        DescribeShareGroupBuilder::deadline_after;
    let submit: fn(DescribeShareGroupBuilder) -> DescribeShareGroup =
        DescribeShareGroupBuilder::submit;
    let _ = (authorized, deadline, submit);
}

#[test]
fn invalid_group_and_zero_deadline_reject_definitely_unsent_at_submit() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));

    for builder in [
        client.admin().describe_share_group(""),
        client
            .admin()
            .describe_share_group("share-workers")
            .deadline_after(Duration::ZERO),
    ] {
        let error = builder
            .submit()
            .wait()
            .err()
            .unwrap_or_else(|| panic!("invalid ShareGroup description must reject"));
        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
    }
}
