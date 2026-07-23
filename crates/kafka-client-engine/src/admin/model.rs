//! Engine-owned public request values translated once into deterministic policy facts.

use kafka_client_core::{
    CreateTopicConfig as CoreConfig, CreateTopicSpecification, CreateTopicsPlan,
    CreateTopicsPlanError,
};

/// One nullable topic configuration entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTopicConfig {
    name: String,
    value: Option<String>,
}

impl CreateTopicConfig {
    /// Creates a configuration entry.
    pub fn new(name: impl Into<String>, value: Option<String>) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }
}

/// One topic in a batched creation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTopic {
    name: String,
    partitions: i32,
    replication_factor: i16,
    configs: Vec<CreateTopicConfig>,
}

impl CreateTopic {
    /// Creates a topic using Kafka's default replication factor.
    pub fn new(name: impl Into<String>, partitions: i32) -> Self {
        Self {
            name: name.into(),
            partitions,
            replication_factor: -1,
            configs: Vec::new(),
        }
    }

    /// Sets an explicit replication factor.
    #[must_use]
    pub const fn with_replication_factor(mut self, replication_factor: i16) -> Self {
        self.replication_factor = replication_factor;
        self
    }

    /// Appends one topic configuration.
    #[must_use]
    pub fn with_config(mut self, config: CreateTopicConfig) -> Self {
        self.configs.push(config);
        self
    }
}

/// One ordered, batch-native `CreateTopics` request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTopicsRequest {
    topics: Vec<CreateTopic>,
    validate_only: bool,
}

impl CreateTopicsRequest {
    /// Creates one ordered batch.
    pub fn new(topics: Vec<CreateTopic>) -> Self {
        Self {
            topics,
            validate_only: false,
        }
    }

    /// Selects broker-side validation without mutation.
    #[must_use]
    pub const fn with_validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }

    pub(crate) fn into_plan(self) -> Result<CreateTopicsPlan, CreateTopicsPlanError> {
        let topics = self
            .topics
            .into_iter()
            .map(|topic| {
                let configs = topic
                    .configs
                    .into_iter()
                    .map(|config| CoreConfig::new(config.name, config.value))
                    .collect();
                CreateTopicSpecification::new(
                    topic.name,
                    topic.partitions,
                    topic.replication_factor,
                    configs,
                )
            })
            .collect();
        CreateTopicsPlan::new(topics, self.validate_only)
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        for topic in &mut self.topics {
            topic.name = canonical_string(std::mem::take(&mut topic.name));
            for config in &mut topic.configs {
                config.name = canonical_string(std::mem::take(&mut config.name));
                config.value = config.value.take().map(canonical_string);
            }
            topic.configs = canonical_vec(std::mem::take(&mut topic.configs));
        }
        self.topics = canonical_vec(self.topics);
        self
    }

    pub(crate) fn retained_charge(&self) -> Option<usize> {
        let mut text_bytes = 0usize;
        let mut config_count = 0usize;
        for topic in &self.topics {
            text_bytes = text_bytes.checked_add(topic.name.len())?;
            config_count = config_count.checked_add(topic.configs.len())?;
            for config in &topic.configs {
                text_bytes = text_bytes.checked_add(config.name.len())?;
                text_bytes =
                    text_bytes.checked_add(config.value.as_ref().map_or(0, String::len))?;
            }
        }
        super::retention::request_charge(self.topics.len(), config_count, text_bytes)
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        self.topics.capacity() == self.topics.len()
            && self.topics.iter().all(|topic| {
                topic.name.capacity() == topic.name.len()
                    && topic.configs.capacity() == topic.configs.len()
                    && topic.configs.iter().all(|config| {
                        config.name.capacity() == config.name.len()
                            && config
                                .value
                                .as_ref()
                                .is_none_or(|value| value.capacity() == value.len())
                    })
            })
    }
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}

fn canonical_vec<T>(value: Vec<T>) -> Vec<T> {
    value.into_boxed_slice().into_vec()
}
