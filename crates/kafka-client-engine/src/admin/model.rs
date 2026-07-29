//! Engine-owned public request values translated once into deterministic policy facts.

use kafka_client_core::{
    CreateTopicConfig as CoreConfig, CreateTopicReplicaAssignment as CoreAssignment,
    CreateTopicSpecification, CreateTopicsPlan, CreateTopicsPlanError,
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

/// One generated-free manual partition placement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTopicReplicaAssignment {
    partition_index: i32,
    broker_ids: Vec<i32>,
}

impl CreateTopicReplicaAssignment {
    /// Creates one unvalidated caller-ordered placement entry.
    pub const fn new(partition_index: i32, broker_ids: Vec<i32>) -> Self {
        Self {
            partition_index,
            broker_ids,
        }
    }

    fn into_core(self) -> CoreAssignment {
        CoreAssignment::new(self.partition_index, self.broker_ids)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CreateTopicPlacement {
    Automatic {
        partitions: i32,
        replication_factor: i16,
    },
    Manual {
        assignments: Vec<CreateTopicReplicaAssignment>,
        conflicting_replication_factor: Option<i16>,
    },
}

/// One topic in a batched creation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTopic {
    name: String,
    placement: CreateTopicPlacement,
    configs: Vec<CreateTopicConfig>,
}

impl CreateTopic {
    /// Creates a topic using Kafka's default replication factor.
    pub fn new(name: impl Into<String>, partitions: i32) -> Self {
        Self {
            name: name.into(),
            placement: CreateTopicPlacement::Automatic {
                partitions,
                replication_factor: -1,
            },
            configs: Vec::new(),
        }
    }

    /// Creates a topic with exact manual replica assignments.
    pub fn with_replica_assignments(
        name: impl Into<String>,
        assignments: Vec<CreateTopicReplicaAssignment>,
        conflicting_replication_factor: Option<i16>,
    ) -> Self {
        Self {
            name: name.into(),
            placement: CreateTopicPlacement::Manual {
                assignments,
                conflicting_replication_factor,
            },
            configs: Vec::new(),
        }
    }

    /// Sets an explicit replication factor.
    #[must_use]
    pub const fn with_replication_factor(mut self, replication_factor: i16) -> Self {
        match &mut self.placement {
            CreateTopicPlacement::Automatic {
                replication_factor: requested,
                ..
            } => *requested = replication_factor,
            CreateTopicPlacement::Manual {
                conflicting_replication_factor,
                ..
            } => *conflicting_replication_factor = Some(replication_factor),
        }
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
                match topic.placement {
                    CreateTopicPlacement::Automatic {
                        partitions,
                        replication_factor,
                    } => CreateTopicSpecification::new(
                        topic.name,
                        partitions,
                        replication_factor,
                        configs,
                    ),
                    CreateTopicPlacement::Manual {
                        assignments,
                        conflicting_replication_factor,
                    } => CreateTopicSpecification::manual(
                        topic.name,
                        assignments
                            .into_iter()
                            .map(CreateTopicReplicaAssignment::into_core)
                            .collect(),
                        conflicting_replication_factor,
                        configs,
                    ),
                }
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
            if let CreateTopicPlacement::Manual { assignments, .. } = &mut topic.placement {
                for assignment in assignments.iter_mut() {
                    assignment.broker_ids =
                        canonical_vec(std::mem::take(&mut assignment.broker_ids));
                }
                *assignments = canonical_vec(std::mem::take(assignments));
            }
        }
        self.topics = canonical_vec(self.topics);
        self
    }

    pub(crate) fn retained_charge(&self) -> Option<usize> {
        let mut text_bytes = 0usize;
        let mut config_count = 0usize;
        let mut assignment_count = 0usize;
        let mut broker_id_count = 0usize;
        for topic in &self.topics {
            text_bytes = text_bytes.checked_add(topic.name.len())?;
            config_count = config_count.checked_add(topic.configs.len())?;
            for config in &topic.configs {
                text_bytes = text_bytes.checked_add(config.name.len())?;
                text_bytes =
                    text_bytes.checked_add(config.value.as_ref().map_or(0, String::len))?;
            }
            if let CreateTopicPlacement::Manual { assignments, .. } = &topic.placement {
                assignment_count = assignment_count.checked_add(assignments.len())?;
                for assignment in assignments {
                    broker_id_count = broker_id_count.checked_add(assignment.broker_ids.len())?;
                }
            }
        }
        super::retention::create_topics_request_charge(
            self.topics.len(),
            config_count,
            assignment_count,
            broker_id_count,
            text_bytes,
        )
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        self.topics.capacity() == self.topics.len()
            && self.topics.iter().all(|topic| {
                topic.name.capacity() == topic.name.len()
                    && topic.configs.capacity() == topic.configs.len()
                    && match &topic.placement {
                        CreateTopicPlacement::Automatic { .. } => true,
                        CreateTopicPlacement::Manual { assignments, .. } => {
                            assignments.capacity() == assignments.len()
                                && assignments.iter().all(|assignment| {
                                    assignment.broker_ids.capacity() == assignment.broker_ids.len()
                                })
                        }
                    }
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
