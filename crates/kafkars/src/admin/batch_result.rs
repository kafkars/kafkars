//! Ordered per-resource outcomes shared by batched public admin operations.

use crate::KafkaError;

/// Deterministically ordered per-resource outcomes for one admin operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchResult<K, V> {
    entries: Vec<(K, Result<V, KafkaError>)>,
}

impl<K, V> BatchResult<K, V> {
    pub(crate) const fn new(entries: Vec<(K, Result<V, KafkaError>)>) -> Self {
        Self { entries }
    }

    /// Returns outcomes in the operation's documented deterministic order.
    pub fn entries(&self) -> &[(K, Result<V, KafkaError>)] {
        &self.entries
    }

    /// Consumes outcomes in the operation's documented deterministic order.
    pub fn into_entries(self) -> Vec<(K, Result<V, KafkaError>)> {
        self.entries
    }
}
