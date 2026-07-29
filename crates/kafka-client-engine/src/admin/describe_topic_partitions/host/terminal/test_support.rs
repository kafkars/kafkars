//! Test-only observations of retained topic-partition page ownership.

use super::super::{AdminDescribeTopicPartitionsHost, AdminDescribeTopicPartitionsHostError};

impl AdminDescribeTopicPartitionsHost {
    pub(in crate::admin::describe_topic_partitions) fn retain_recovered_call_for_test(&mut self) {
        self.operations[0].recovered_call =
            Some(crate::driver::RecoveredDescribeTopicPartitionsCall::for_test());
    }

    pub(in crate::admin::describe_topic_partitions) fn recovered_ownership_is_retained_for_test(
        &self,
    ) -> bool {
        self.operations[0].recovered_call.is_some()
    }

    pub(in crate::admin::describe_topic_partitions) fn settle_recovered_transport_for_test(
        &mut self,
    ) -> Result<(), AdminDescribeTopicPartitionsHostError> {
        self.settle_recovered_transport(0)
    }

    pub(in crate::admin::describe_topic_partitions) fn publish_terminal_for_test(
        &mut self,
    ) -> Result<(), AdminDescribeTopicPartitionsHostError> {
        self.publish_terminal(0)
    }
}
