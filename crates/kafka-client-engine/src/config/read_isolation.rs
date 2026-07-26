//! Stable engine configuration translated once into deterministic consumer policy.

/// Application-record visibility selected before a consumer owner starts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ConsumerReadIsolation {
    /// Deliver every application record accepted by the partition log.
    #[default]
    ReadUncommitted,
    /// Deliver only nontransactional or committed transactional records.
    ReadCommitted,
}

impl ConsumerReadIsolation {
    pub(crate) const fn core(self) -> kafka_client_core::ReadIsolation {
        match self {
            Self::ReadUncommitted => kafka_client_core::ReadIsolation::ReadUncommitted,
            Self::ReadCommitted => kafka_client_core::ReadIsolation::ReadCommitted,
        }
    }
}
