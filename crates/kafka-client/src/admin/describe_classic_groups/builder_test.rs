//! Public classic-group builder trait and submission-shape tests.

use std::time::Duration;

use super::{DescribeClassicGroups, DescribeClassicGroupsBuilder};
use crate::{Client, DeliveryStatus, ErrorKind};

#[test]
fn builder_is_thread_safe_and_exposes_one_submission_boundary() {
    fn assert_send_sync_debug<T: Send + Sync + std::fmt::Debug>() {}
    assert_send_sync_debug::<DescribeClassicGroupsBuilder>();

    let authorized: fn(DescribeClassicGroupsBuilder, bool) -> DescribeClassicGroupsBuilder =
        DescribeClassicGroupsBuilder::include_authorized_operations;
    let deadline: fn(DescribeClassicGroupsBuilder, Duration) -> DescribeClassicGroupsBuilder =
        DescribeClassicGroupsBuilder::deadline_after;
    let submit: fn(DescribeClassicGroupsBuilder) -> DescribeClassicGroups =
        DescribeClassicGroupsBuilder::submit;

    let _ = (authorized, deadline, submit);
}

#[test]
fn public_handle_keeps_zero_deadline_inert_until_submit() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));
    let error = client
        .admin()
        .describe_classic_groups(["classic-workers"])
        .deadline_after(Duration::ZERO)
        .submit()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("zero deadline must reject at submit"));

    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}
