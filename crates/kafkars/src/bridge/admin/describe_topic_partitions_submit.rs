//! Admission handoff for public Admin `DescribeTopicPartitions`.

use std::time::Duration;

use super::AdminEngine;
use crate::bridge::describe_topic_partitions::{
    AdminDescribeTopicPartitions, DescribeTopicPartitionsAdminRequest,
};

impl AdminEngine {
    pub(crate) fn submit_describe_topic_partitions(
        &self,
        request: DescribeTopicPartitionsAdminRequest,
        timeout: Duration,
    ) -> AdminDescribeTopicPartitions {
        AdminDescribeTopicPartitions::from_admission(
            self.handle
                .try_describe_topic_partitions(request.into_engine(), timeout),
        )
    }
}
