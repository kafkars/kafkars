//! Inert metadata-quorum builder surface tests.

use std::{future::Future, time::Duration};

use super::{DescribeMetadataQuorum, DescribeMetadataQuorumBuilder};

fn assert_future<
    T: Future<Output = Result<super::MetadataQuorumDescription, crate::KafkaError>>,
>() {
}

#[test]
fn operation_is_a_named_runtime_neutral_future() {
    assert_future::<DescribeMetadataQuorum>();
}

#[test]
fn builder_surface_keeps_timeout_configuration_inert() {
    let method: fn(DescribeMetadataQuorumBuilder, Duration) -> DescribeMetadataQuorumBuilder =
        DescribeMetadataQuorumBuilder::deadline_after;
    let submit: fn(DescribeMetadataQuorumBuilder) -> DescribeMetadataQuorum =
        DescribeMetadataQuorumBuilder::submit;

    let _ = (method, submit);
}
