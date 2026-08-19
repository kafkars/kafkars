//! `ShareGroup` offset-listing builder surface tests.

use std::{future::Future, time::Duration};

use crate::TopicPartition;

use super::{ListShareGroupOffsets, ListShareGroupOffsetsBuilder, ListShareGroupOffsetsResult};

fn assert_future<T: Future<Output = Result<ListShareGroupOffsetsResult, crate::KafkaError>>>() {}

#[test]
fn operation_is_one_named_future() {
    assert_future::<ListShareGroupOffsets>();
}

#[test]
fn builder_exposes_selection_deadline_and_submission() {
    let partitions: fn(
        ListShareGroupOffsetsBuilder,
        Vec<TopicPartition>,
    ) -> ListShareGroupOffsetsBuilder = ListShareGroupOffsetsBuilder::partitions;
    let deadline: fn(ListShareGroupOffsetsBuilder, Duration) -> ListShareGroupOffsetsBuilder =
        ListShareGroupOffsetsBuilder::deadline_after;
    let submit: fn(ListShareGroupOffsetsBuilder) -> ListShareGroupOffsets =
        ListShareGroupOffsetsBuilder::submit;

    let _ = (partitions, deadline, submit);
}
