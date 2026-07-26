//! Validated semantic input for one consumer-group offset query.

use core::fmt;

/// Maximum UTF-8 byte length accepted for one group coordinator key.
pub(super) const MAX_CONSUMER_GROUP_ID_BYTES: usize = i16::MAX as usize;

/// Validated intent for one all-partition consumer-group offset query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListConsumerGroupOffsetsPlan {
    group_id: String,
    require_stable: bool,
}

impl ListConsumerGroupOffsetsPlan {
    /// Validates one explicit group identity and its stability requirement.
    pub fn new(
        group_id: String,
        require_stable: bool,
    ) -> Result<Self, ListConsumerGroupOffsetsPlanError> {
        if group_id.is_empty() {
            return Err(ListConsumerGroupOffsetsPlanError::EmptyGroupId);
        }
        if group_id.len() > MAX_CONSUMER_GROUP_ID_BYTES {
            return Err(ListConsumerGroupOffsetsPlanError::GroupIdTooLong);
        }
        Ok(Self {
            group_id,
            require_stable,
        })
    }

    /// Returns the exact UTF-8 group identity.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns whether Kafka must reject unstable group state.
    pub const fn require_stable(&self) -> bool {
        self.require_stable
    }
}

/// Invalid deterministic group-offset query intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListConsumerGroupOffsetsPlanError {
    /// The query must name one explicit consumer group.
    EmptyGroupId,
    /// The UTF-8 group identity cannot fit the coordinator key domain.
    GroupIdTooLong,
}

impl fmt::Display for ListConsumerGroupOffsetsPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyGroupId => "consumer group id is empty",
            Self::GroupIdTooLong => "consumer group id exceeds the coordinator key limit",
        })
    }
}

impl std::error::Error for ListConsumerGroupOffsetsPlanError {}
