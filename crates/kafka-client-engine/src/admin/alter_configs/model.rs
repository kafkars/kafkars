//! Engine-owned request strings and retained-capacity facts for incremental changes.

use kafka_client_core::{
    ConfigAlteration as CoreAlteration, IncrementalAlterConfigsPlan,
    IncrementalAlterConfigsPlanError, TopicConfigAlteration as CoreTopicAlteration,
};

use crate::admin::retention::{
    RESULT_DIAGNOSTIC_BYTES_PER_TOPIC, request_charge, result_fixed_charge,
};

/// One exact incremental configuration operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IncrementalConfigOperation {
    /// Replaces the current value.
    Set(String),
    /// Removes the explicit value.
    Delete,
    /// Appends using Kafka's configuration semantics.
    Append(String),
    /// Subtracts using Kafka's configuration semantics.
    Subtract(String),
}

impl IncrementalConfigOperation {
    fn canonicalize(self) -> Self {
        match self {
            Self::Set(value) => Self::Set(canonical_string(value)),
            Self::Delete => Self::Delete,
            Self::Append(value) => Self::Append(canonical_string(value)),
            Self::Subtract(value) => Self::Subtract(canonical_string(value)),
        }
    }

    fn value_bytes(&self) -> usize {
        match self {
            Self::Set(value) | Self::Append(value) | Self::Subtract(value) => value.len(),
            Self::Delete => 0,
        }
    }

    fn into_core(self, key: String) -> CoreAlteration {
        match self {
            Self::Set(value) => CoreAlteration::set(key, value),
            Self::Delete => CoreAlteration::delete(key),
            Self::Append(value) => CoreAlteration::append(key, value),
            Self::Subtract(value) => CoreAlteration::subtract(key, value),
        }
    }
}

/// One named configuration change.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncrementalConfigAlteration {
    key: String,
    operation: IncrementalConfigOperation,
}

impl IncrementalConfigAlteration {
    /// Creates one raw alteration for validation at admission.
    pub const fn new(key: String, operation: IncrementalConfigOperation) -> Self {
        Self { key, operation }
    }

    fn canonicalize(mut self) -> Self {
        self.key = canonical_string(self.key);
        self.operation = self.operation.canonicalize();
        self
    }

    fn text_bytes(&self) -> Option<usize> {
        self.key.len().checked_add(self.operation.value_bytes())
    }

    fn into_core(self) -> CoreAlteration {
        self.operation.into_core(self.key)
    }
}

/// One topic and its caller-ordered changes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicConfigAlterations {
    topic: String,
    alterations: Vec<IncrementalConfigAlteration>,
}

impl TopicConfigAlterations {
    /// Creates one raw topic change set.
    pub const fn new(topic: String, alterations: Vec<IncrementalConfigAlteration>) -> Self {
        Self { topic, alterations }
    }

    fn canonicalize(mut self) -> Self {
        self.topic = canonical_string(self.topic);
        self.alterations = canonical_vec(
            self.alterations
                .into_iter()
                .map(IncrementalConfigAlteration::canonicalize)
                .collect(),
        );
        self
    }

    fn text_bytes(&self) -> Option<usize> {
        self.alterations
            .iter()
            .try_fold(self.topic.len(), |bytes, alteration| {
                bytes.checked_add(alteration.text_bytes()?)
            })
    }

    fn into_core(self) -> CoreTopicAlteration {
        CoreTopicAlteration::new(
            self.topic,
            self.alterations
                .into_iter()
                .map(IncrementalConfigAlteration::into_core)
                .collect(),
        )
    }
}

/// One ordered topic-only incremental configuration request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncrementalAlterConfigsRequest {
    topics: Vec<TopicConfigAlterations>,
    validate_only: bool,
}

impl IncrementalAlterConfigsRequest {
    /// Creates one ordered request.
    pub const fn new(topics: Vec<TopicConfigAlterations>) -> Self {
        Self {
            topics,
            validate_only: false,
        }
    }

    /// Selects broker validation without mutation.
    #[must_use]
    pub const fn with_validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.topics = canonical_vec(
            self.topics
                .into_iter()
                .map(TopicConfigAlterations::canonicalize)
                .collect(),
        );
        self
    }

    pub(crate) fn retention(&self) -> Option<IncrementalAlterConfigsRetention> {
        let topic_count = self.topics.len();
        let alteration_count = self.topics.iter().try_fold(0usize, |count, topic| {
            count.checked_add(topic.alterations.len())
        })?;
        let text_bytes = self.topics.iter().try_fold(0usize, |bytes, topic| {
            bytes.checked_add(topic.text_bytes()?)
        })?;
        let topic_bytes = self
            .topics
            .iter()
            .try_fold(0usize, |bytes, topic| bytes.checked_add(topic.topic.len()))?;
        let request = request_charge(topic_count, alteration_count, text_bytes)?;
        let result_limit = result_fixed_charge(topic_count, topic_bytes)?
            .checked_add(topic_count.checked_mul(RESULT_DIAGNOSTIC_BYTES_PER_TOPIC)?)?;
        Some(IncrementalAlterConfigsRetention {
            total: request,
            result_limit,
        })
    }

    pub(crate) fn into_plan(
        self,
    ) -> Result<IncrementalAlterConfigsPlan, IncrementalAlterConfigsPlanError> {
        IncrementalAlterConfigsPlan::new(
            self.topics
                .into_iter()
                .map(TopicConfigAlterations::into_core)
                .collect(),
            self.validate_only,
        )
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        self.topics.capacity() == self.topics.len()
            && self.topics.iter().all(|topic| {
                topic.topic.capacity() == topic.topic.len()
                    && topic.alterations.capacity() == topic.alterations.len()
                    && topic
                        .alterations
                        .iter()
                        .all(|alteration| alteration.key.capacity() == alteration.key.len())
            })
    }
}

#[derive(Clone, Copy)]
pub(crate) struct IncrementalAlterConfigsRetention {
    total: usize,
    result_limit: usize,
}

impl IncrementalAlterConfigsRetention {
    pub(crate) const fn total(self) -> usize {
        self.total
    }

    pub(crate) const fn result_limit(self) -> usize {
        self.result_limit
    }

    #[cfg(test)]
    pub(crate) const fn from_parts(total: usize, result_limit: usize) -> Self {
        Self {
            total,
            result_limit,
        }
    }
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}

fn canonical_vec<T>(value: Vec<T>) -> Vec<T> {
    value.into_boxed_slice().into_vec()
}
