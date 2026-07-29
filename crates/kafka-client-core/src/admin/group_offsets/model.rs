//! Declarative facade for validated consumer-group offset query intent.

mod error;
mod plan;
mod query;

#[cfg(test)]
mod plan_test;
#[cfg(test)]
mod query_test;

pub use error::ListConsumerGroupOffsetsPlanError;
pub use plan::ListConsumerGroupOffsetsPlan;
pub(crate) use plan::ListConsumerGroupOffsetsPlanShape;
pub use query::{
    ListConsumerGroupOffsetTarget, ListConsumerGroupOffsetsQuery, ListConsumerGroupOffsetsSelection,
};

/// Maximum UTF-8 byte length accepted for one group coordinator key.
pub(super) const MAX_CONSUMER_GROUP_ID_BYTES: usize = i16::MAX as usize;
/// Maximum group identities retained by one accepted batch operation.
pub(super) const MAX_CONSUMER_GROUPS: usize = 16 * 1024;
/// Maximum selected topic-partitions retained by one accepted operation.
pub(super) const MAX_SELECTED_PARTITIONS: usize = 4 * 1024;
/// Maximum aggregate request text retained by one batch operation.
pub(super) const MAX_CONSUMER_GROUP_REQUEST_TEXT_BYTES: usize = 1024 * 1024;
