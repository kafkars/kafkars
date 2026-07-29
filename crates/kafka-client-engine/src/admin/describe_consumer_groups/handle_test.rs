//! Shared-port modern-first and direct-classic admission surface evidence.

use std::time::Duration;

use crate::admin::AdminHandle;

use super::{
    DescribeConsumerGroupsAccepted, DescribeConsumerGroupsAdmissionError,
    DescribeConsumerGroupsRequest,
};

#[test]
fn modern_and_classic_entry_points_share_the_same_request_and_terminal_types() {
    type EntryPoint =
        fn(
            &AdminHandle,
            DescribeConsumerGroupsRequest,
            Duration,
        ) -> Result<DescribeConsumerGroupsAccepted, DescribeConsumerGroupsAdmissionError>;

    let modern: EntryPoint = AdminHandle::try_describe_consumer_groups;
    let classic: EntryPoint = AdminHandle::try_describe_classic_groups;
    let _ = (modern, classic);
}
