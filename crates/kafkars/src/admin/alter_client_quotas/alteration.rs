//! Stable caller-ordered client-quota alteration vocabulary.

use super::ClientQuotaEntity;

/// One exact client-quota key operation.
#[derive(Clone, Debug, PartialEq)]
pub enum ClientQuotaAlterationOperation {
    /// Replaces the named quota with one finite numeric value.
    Set {
        /// Kafka's quota configuration key.
        key: String,
        /// The exact requested numeric value.
        value: f64,
    },
    /// Removes the named quota override.
    Remove {
        /// Kafka's quota configuration key.
        key: String,
    },
}

impl ClientQuotaAlterationOperation {
    /// Replaces one quota value.
    pub fn set(key: impl Into<String>, value: f64) -> Self {
        Self::Set {
            key: key.into(),
            value,
        }
    }

    /// Removes one quota override.
    pub fn remove(key: impl Into<String>) -> Self {
        Self::Remove { key: key.into() }
    }

    /// Returns Kafka's quota configuration key.
    pub fn key(&self) -> &str {
        match self {
            Self::Set { key, .. } | Self::Remove { key } => key,
        }
    }

    /// Returns the replacement value, or `None` for removal.
    pub const fn value(&self) -> Option<f64> {
        match self {
            Self::Set { value, .. } => Some(*value),
            Self::Remove { .. } => None,
        }
    }
}

/// One client-quota entity and its caller-ordered nonempty operation set.
///
/// Construction is inert. Empty operation sets, duplicate keys, and numeric
/// bounds are rejected only when the surrounding builder is submitted.
#[derive(Clone, Debug, PartialEq)]
pub struct ClientQuotaAlteration {
    entity: ClientQuotaEntity,
    operations: Vec<ClientQuotaAlterationOperation>,
}

impl ClientQuotaAlteration {
    /// Creates one inert alteration in caller operation order.
    pub fn new<I>(entity: ClientQuotaEntity, operations: I) -> Self
    where
        I: IntoIterator<Item = ClientQuotaAlterationOperation>,
    {
        Self {
            entity,
            operations: operations.into_iter().collect(),
        }
    }

    /// Returns the canonical entity identity.
    pub const fn entity(&self) -> &ClientQuotaEntity {
        &self.entity
    }

    /// Returns operations in caller order.
    pub fn operations(&self) -> &[ClientQuotaAlterationOperation] {
        &self.operations
    }

    pub(crate) fn into_parts(self) -> (ClientQuotaEntity, Vec<ClientQuotaAlterationOperation>) {
        (self.entity, self.operations)
    }
}
