//! Engine-owned request selection for one bounded `DescribeTopics` operation.

use kafka_client_core::{DescribeTopicsPlan, DescribeTopicsPlanError};

const RESULT_BYTES_PER_TOPIC: usize = 128 * 1024;

/// One explicit name-based or all-topic `DescribeTopics` request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeTopicsRequest {
    selection: DescribeTopicsRequestSelection,
    include_authorized_operations: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DescribeTopicsRequestSelection {
    Named(Vec<String>),
    Ids(Vec<[u8; 16]>),
    All { include_internal: bool },
}

impl DescribeTopicsRequest {
    /// Creates one ordered batch of topic names.
    pub const fn new(topics: Vec<String>) -> Self {
        Self {
            selection: DescribeTopicsRequestSelection::Named(topics),
            include_authorized_operations: false,
        }
    }

    /// Creates one ordered batch of topic IDs.
    pub const fn by_ids(topic_ids: Vec<[u8; 16]>) -> Self {
        Self {
            selection: DescribeTopicsRequestSelection::Ids(topic_ids),
            include_authorized_operations: false,
        }
    }

    /// Creates an all-topic query over broker-visible topic descriptions.
    pub const fn all(include_internal: bool) -> Self {
        Self {
            selection: DescribeTopicsRequestSelection::All { include_internal },
            include_authorized_operations: false,
        }
    }

    /// Replaces internal-topic retention for an all-topic selection.
    pub const fn with_include_internal(mut self, include: bool) -> Self {
        if let DescribeTopicsRequestSelection::All { include_internal } = &mut self.selection {
            *include_internal = include;
        }
        self
    }

    /// Selects whether Kafka should return exact topic authorization bitfields.
    pub const fn with_authorized_operations(mut self, include: bool) -> Self {
        self.include_authorized_operations = include;
        self
    }

    pub(crate) fn into_plan(self) -> Result<DescribeTopicsPlan, DescribeTopicsPlanError> {
        let plan = match self.selection {
            DescribeTopicsRequestSelection::Named(topics) => DescribeTopicsPlan::new(topics),
            DescribeTopicsRequestSelection::Ids(topic_ids) => DescribeTopicsPlan::by_ids(topic_ids),
            DescribeTopicsRequestSelection::All { include_internal } => {
                Ok(DescribeTopicsPlan::all(include_internal))
            }
        }?;
        Ok(plan.with_authorized_operations(self.include_authorized_operations))
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        if let DescribeTopicsRequestSelection::Named(topics) = &mut self.selection {
            for topic in topics.iter_mut() {
                *topic = canonical_string(std::mem::take(topic));
            }
            *topics = std::mem::take(topics).into_boxed_slice().into_vec();
        }
        self
    }

    pub(crate) fn retained_charge(&self) -> Option<usize> {
        match &self.selection {
            DescribeTopicsRequestSelection::Named(topics) => {
                let text_bytes = topics
                    .iter()
                    .try_fold(0usize, |bytes, topic| bytes.checked_add(topic.len()))?;
                let request = crate::admin::retention::request_charge(topics.len(), 0, text_bytes)?;
                request.checked_add(topics.len().checked_mul(RESULT_BYTES_PER_TOPIC)?)
            }
            DescribeTopicsRequestSelection::Ids(topic_ids) => {
                let request = crate::admin::retention::request_charge(topic_ids.len(), 0, 0)?;
                request.checked_add(topic_ids.len().checked_mul(RESULT_BYTES_PER_TOPIC)?)
            }
            DescribeTopicsRequestSelection::All { .. } => {
                Some(super::limits::all_topics_retained_charge())
            }
        }
    }
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}
