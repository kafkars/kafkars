//! Deterministic sticky selection among driver-known available partitions.

use core::fmt;

use crate::PartitionIndex;

use super::{PartitionSelection, TopicPartitionFacts, TopicPartitionFactsError};

/// Per-topic unkeyed selection state advanced only when its batch seals.
#[derive(Debug)]
pub struct StickyPartitioner {
    current: Option<PartitionIndex>,
    cursor: u64,
}

impl StickyPartitioner {
    /// Creates per-topic sticky state from caller-owned deterministic dispersion.
    pub const fn new(initial_cursor: u64) -> Self {
        Self {
            current: None,
            cursor: initial_cursor,
        }
    }

    /// Selects or reuses one currently available partition.
    ///
    /// Repeated calls remain on the current partition while it is available.
    /// A metadata view that loses that partition causes immediate deterministic
    /// reselection. The supplied facts remain borrowed and are never retained.
    pub fn select(
        &mut self,
        facts: TopicPartitionFacts<'_>,
    ) -> Result<PartitionSelection, StickyPartitionError> {
        if let Some(current) = self.current {
            if let Some(fact) = facts
                .find_available(current)
                .map_err(StickyPartitionError::IncoherentTopicFacts)?
            {
                return Ok(facts.select_available(fact));
            }
        }

        let available_len = facts.available_len();
        if available_len == 0 {
            return Err(StickyPartitionError::NoAvailablePartition);
        }
        let available_len = u64::try_from(available_len)
            .map_err(|_| StickyPartitionError::AvailableSetUnrepresentable)?;
        let selected_index = usize::try_from(self.cursor % available_len)
            .map_err(|_| StickyPartitionError::AvailableSetUnrepresentable)?;
        let fact = facts
            .available_at(selected_index)
            .map_err(StickyPartitionError::IncoherentTopicFacts)?;
        self.current = Some(fact.partition());
        Ok(facts.select_available(fact))
    }

    /// Ends the current sticky batch so the next selection advances fairly.
    pub fn batch_sealed(&mut self) {
        if self.current.take().is_some() {
            self.cursor = self.cursor.wrapping_add(1);
        }
    }
}

/// Failure to select an unkeyed partition from current topic facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StickyPartitionError {
    /// No logical partition currently has a known leader.
    NoAvailablePartition,
    /// The supplied lazy topic view violated its normalized fact contract.
    IncoherentTopicFacts(TopicPartitionFactsError),
    /// The available set cannot be indexed portably by deterministic policy.
    AvailableSetUnrepresentable,
}

impl fmt::Display for StickyPartitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoAvailablePartition => {
                formatter.write_str("topic has no currently available partition")
            }
            Self::IncoherentTopicFacts(source) => {
                write!(formatter, "incoherent topic partition facts: {source}")
            }
            Self::AvailableSetUnrepresentable => {
                formatter.write_str("available partition set is not portably representable")
            }
        }
    }
}

impl std::error::Error for StickyPartitionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NoAvailablePartition | Self::AvailableSetUnrepresentable => None,
            Self::IncoherentTopicFacts(source) => Some(source),
        }
    }
}
