//! Public builder selection and result thread-safety shape tests.

use std::time::Duration;

use crate::{Client, DeliveryStatus, ErrorKind, TopicPartition};

use super::{DescribeLogDirs, DescribeLogDirsBuilder};

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn builder_and_result_are_send_sync_without_runtime_types() {
    assert_send_sync::<super::DescribeLogDirsBuilder>();
    assert_send_sync::<super::DescribeLogDirsResult>();
}

#[test]
fn builder_exposes_inert_selection_deadline_and_submission() {
    let partitions: fn(DescribeLogDirsBuilder, Vec<TopicPartition>) -> DescribeLogDirsBuilder =
        DescribeLogDirsBuilder::partitions;
    let deadline: fn(DescribeLogDirsBuilder, Duration) -> DescribeLogDirsBuilder =
        DescribeLogDirsBuilder::deadline_after;
    let submit: fn(DescribeLogDirsBuilder) -> DescribeLogDirs = DescribeLogDirsBuilder::submit;

    let _ = (partitions, deadline, submit);
}

#[test]
fn empty_explicit_selection_is_rejected_only_at_submission() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));
    let builder = client
        .admin()
        .describe_log_dirs([7])
        .partitions(Vec::<TopicPartition>::new());

    let error = builder
        .submit()
        .wait()
        .expect_err("empty explicit selection must reject at submit");
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}
