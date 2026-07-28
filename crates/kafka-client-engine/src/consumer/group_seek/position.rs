//! Stable group-consumer seek position independent of core representation.

use kafka_client_core::{NextFetchOffset, StartPosition};

/// Replacement position for one assigned classic-group partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerSeekPosition {
    /// Resolve Kafka's earliest available offset.
    Beginning,
    /// Resolve Kafka's current end offset.
    End,
    /// Begin the next Fetch at this exact nonnegative offset.
    Offset(i64),
}

impl GroupConsumerSeekPosition {
    pub(in crate::consumer) const fn try_into_core(self) -> Option<StartPosition> {
        match self {
            Self::Beginning => Some(StartPosition::Beginning),
            Self::End => Some(StartPosition::End),
            Self::Offset(offset) => match NextFetchOffset::try_from_raw(offset) {
                Some(offset) => Some(StartPosition::Offset(offset)),
                None => None,
            },
        }
    }
}
