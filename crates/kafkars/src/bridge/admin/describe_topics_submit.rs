//! `DescribeTopics` admission under duration and fixed-deadline boundaries.

use std::time::{Duration, Instant};

use super::AdminEngine;
use crate::bridge::{
    admin_topics_by_id_operation::AdminDescribeTopicsById,
    admin_topics_operation::AdminDescribeTopics, admin_topics_request::DescribeTopicsAdminRequest,
};

impl AdminEngine {
    pub(crate) fn submit_describe_topics(
        &self,
        request: DescribeTopicsAdminRequest,
        timeout: Duration,
    ) -> AdminDescribeTopics {
        AdminDescribeTopics::from_admission(
            self.handle
                .try_describe_topics(request.into_engine(), timeout),
        )
    }

    pub(crate) fn submit_describe_topics_until(
        &self,
        request: DescribeTopicsAdminRequest,
        deadline: Instant,
    ) -> AdminDescribeTopics {
        AdminDescribeTopics::from_admission(
            self.handle
                .try_describe_topics_until(request.into_engine(), deadline),
        )
    }

    pub(crate) fn submit_describe_topics_by_id(
        &self,
        request: DescribeTopicsAdminRequest,
        timeout: Duration,
    ) -> AdminDescribeTopicsById {
        AdminDescribeTopicsById::from_admission(
            self.handle
                .try_describe_topics(request.into_engine(), timeout),
        )
    }
}
