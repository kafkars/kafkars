//! Engine-owned scalar intent for one Admin `DeleteRecords` query.

use kafka_client_core::{DeleteRecordsPlan, DeleteRecordsPlanError, DeleteRecordsTarget};

/// One engine-owned topic-partition request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteRecordsRequestTarget {
    topic: String,
    partition: i32,
    before_offset: i64,
}

impl DeleteRecordsRequestTarget {
    /// Creates one inert target for validation at admission.
    pub const fn new(topic: String, partition: i32, before_offset: i64) -> Self {
        Self {
            topic,
            partition,
            before_offset,
        }
    }

    /// Consumes the target into stable scalar values.
    pub fn into_parts(self) -> (String, i32, i64) {
        (self.topic, self.partition, self.before_offset)
    }

    fn canonicalize(mut self) -> Self {
        self.topic = self.topic.into_boxed_str().into_string();
        self
    }

    fn into_core(self) -> DeleteRecordsTarget {
        let (topic, partition, before_offset) = self.into_parts();
        DeleteRecordsTarget::new(topic, partition, before_offset)
    }
}

/// One caller-ordered bounded request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteRecordsRequest {
    targets: Vec<DeleteRecordsRequestTarget>,
}

impl DeleteRecordsRequest {
    /// Creates inert request intent for validation at the operation boundary.
    pub const fn new(targets: Vec<DeleteRecordsRequestTarget>) -> Self {
        Self { targets }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.targets = self
            .targets
            .into_iter()
            .map(DeleteRecordsRequestTarget::canonicalize)
            .collect();
        self.targets.shrink_to_fit();
        self
    }

    pub(crate) fn into_plan(self) -> Result<DeleteRecordsPlan, DeleteRecordsPlanError> {
        DeleteRecordsPlan::new(
            self.targets
                .into_iter()
                .map(DeleteRecordsRequestTarget::into_core)
                .collect(),
        )
    }
}
