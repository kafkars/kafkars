//! Engine-owned scalar intent for one Admin `DescribeProducers` query.

use kafka_client_core::{
    AdminDescribeProducerTarget, AdminDescribeProducersPlan, AdminDescribeProducersPlanError,
};

/// One engine-owned topic-partition target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeProducersRequestTarget {
    topic: String,
    partition: i32,
}

impl AdminDescribeProducersRequestTarget {
    /// Creates inert target intent for validation at admission.
    pub const fn new(topic: String, partition: i32) -> Self {
        Self { topic, partition }
    }

    fn canonicalize(mut self) -> Self {
        self.topic = self.topic.into_boxed_str().into_string();
        self
    }

    fn into_core(self) -> AdminDescribeProducerTarget {
        AdminDescribeProducerTarget::new(self.topic, self.partition)
    }
}

/// One caller-ordered bounded producer-description request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminDescribeProducersRequest {
    targets: Vec<AdminDescribeProducersRequestTarget>,
    broker_id: Option<i32>,
}

impl AdminDescribeProducersRequest {
    /// Creates inert request intent for validation at the operation boundary.
    pub const fn new(
        targets: Vec<AdminDescribeProducersRequestTarget>,
        broker_id: Option<i32>,
    ) -> Self {
        Self { targets, broker_id }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.targets = self
            .targets
            .into_iter()
            .map(AdminDescribeProducersRequestTarget::canonicalize)
            .collect();
        self.targets.shrink_to_fit();
        self
    }

    pub(crate) fn into_plan(
        self,
    ) -> Result<AdminDescribeProducersPlan, AdminDescribeProducersPlanError> {
        AdminDescribeProducersPlan::new(
            self.targets
                .into_iter()
                .map(AdminDescribeProducersRequestTarget::into_core)
                .collect(),
            self.broker_id,
        )
    }
}
