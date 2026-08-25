//! Facade-owned topic-identity bindings and commit-validation revision fence.

use crate::{ErrorKind, KafkaError, TopicUuid};

/// One pre-admission identity mutation committed only after engine acceptance.
pub(super) struct PreparedIdentityMutation {
    next_revision: u64,
    new_binding: Option<TransactionTopicBinding>,
}

/// Exact expected identity for one topic used by the active transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct TransactionTopicBinding {
    topic: String,
    topic_uuid: Option<TopicUuid>,
}

impl TransactionTopicBinding {
    pub(super) fn topic(&self) -> &str {
        &self.topic
    }

    pub(super) const fn topic_uuid(&self) -> Option<TopicUuid> {
        self.topic_uuid
    }
}

/// Monotonic mutation revision and exact topic set for one facade transaction.
#[derive(Debug)]
pub(super) struct TransactionIdentityState {
    revision: u64,
    sealed_revision: Option<u64>,
    topic_mismatch: bool,
    topics: Vec<TransactionTopicBinding>,
}

impl TransactionIdentityState {
    pub(super) const fn new() -> Self {
        Self {
            revision: 0,
            sealed_revision: None,
            topic_mismatch: false,
            topics: Vec::new(),
        }
    }

    pub(super) fn prepare_mutation(
        &mut self,
        topic: Option<(&str, Option<TopicUuid>)>,
    ) -> Result<PreparedIdentityMutation, KafkaError> {
        let next_revision = self.revision.checked_add(1).ok_or_else(|| {
            KafkaError::new(
                ErrorKind::Internal,
                "transaction identity revision exhausted",
            )
        })?;
        let new_binding = match topic {
            None => None,
            Some((topic, topic_uuid)) => self.prepare_binding(topic, topic_uuid)?,
        };
        if new_binding.is_some() {
            self.topics.try_reserve_exact(1).map_err(|_| {
                KafkaError::new(
                    ErrorKind::Internal,
                    "transaction topic-binding capacity allocation failed",
                )
            })?;
        }
        Ok(PreparedIdentityMutation {
            next_revision,
            new_binding,
        })
    }

    pub(super) fn commit_mutation(&mut self, prepared: PreparedIdentityMutation) {
        if let Some(binding) = prepared.new_binding {
            self.topics.push(binding);
        }
        self.revision = prepared.next_revision;
        self.sealed_revision = None;
    }

    pub(super) fn topics(&self) -> &[TransactionTopicBinding] {
        &self.topics
    }

    pub(super) fn topic_names(&self) -> Result<Vec<String>, KafkaError> {
        let mut names = Vec::new();
        names.try_reserve_exact(self.topics.len()).map_err(|_| {
            KafkaError::new(
                ErrorKind::Internal,
                "transaction validation topic-list allocation failed",
            )
        })?;
        for binding in &self.topics {
            if binding.topic_uuid.is_none() {
                continue;
            }
            let mut name = String::new();
            name.try_reserve_exact(binding.topic.len()).map_err(|_| {
                KafkaError::new(
                    ErrorKind::Internal,
                    "transaction validation topic-name allocation failed",
                )
            })?;
            name.push_str(&binding.topic);
            names.push(name);
        }
        Ok(names)
    }

    pub(super) const fn revision(&self) -> u64 {
        self.revision
    }

    pub(super) fn install_seal(&mut self, revision: u64) -> Result<(), KafkaError> {
        if self.topic_mismatch {
            return Err(KafkaError::new(
                ErrorKind::Identity,
                "transaction topic identity mismatch requires abort",
            )
            .with_transaction_abort_required());
        }
        if revision != self.revision {
            return Err(KafkaError::new(
                ErrorKind::State,
                "transaction changed while topic identities were validated",
            ));
        }
        self.sealed_revision = Some(revision);
        Ok(())
    }

    pub(super) fn requires_validation(&self) -> bool {
        self.topics
            .iter()
            .any(|binding| binding.topic_uuid.is_some())
    }

    pub(super) fn is_sealed(&self) -> bool {
        !self.topic_mismatch && self.sealed_revision == Some(self.revision)
    }

    pub(super) const fn topic_mismatch(&self) -> bool {
        self.topic_mismatch
    }

    pub(super) fn mark_topic_mismatch(&mut self) {
        self.topic_mismatch = true;
        self.sealed_revision = None;
    }

    fn prepare_binding(
        &self,
        topic: &str,
        topic_uuid: Option<TopicUuid>,
    ) -> Result<Option<TransactionTopicBinding>, KafkaError> {
        if let Some(binding) = self.topics.iter().find(|binding| binding.topic == topic) {
            return if binding.topic_uuid == topic_uuid {
                Ok(None)
            } else {
                Err(KafkaError::new(
                    ErrorKind::Identity,
                    "transaction topic name changed UUID or mixed bound and unbound identities",
                ))
            };
        }
        let mut owned_topic = String::new();
        owned_topic.try_reserve_exact(topic.len()).map_err(|_| {
            KafkaError::new(
                ErrorKind::Internal,
                "transaction topic-identity allocation failed",
            )
        })?;
        owned_topic.push_str(topic);
        Ok(Some(TransactionTopicBinding {
            topic: owned_topic,
            topic_uuid,
        }))
    }
}
