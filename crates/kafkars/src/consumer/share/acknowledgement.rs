//! Public share acknowledgement values and linear capability ownership.

use crate::bridge::share_consumer::{
    ShareAcknowledgement as BridgeAcknowledgement, ShareDisposition as BridgeDisposition,
    ShareRecordDecision as BridgeDecision,
};

/// Application disposition for one acquired share record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareDisposition {
    /// Processing succeeded and normal redelivery must stop.
    Accept,
    /// Processing may succeed later and the record should become available.
    Release,
    /// Processing is permanently rejected and normal redelivery must stop.
    Reject,
}

impl ShareDisposition {
    pub(super) const fn into_bridge(self) -> BridgeDisposition {
        match self {
            Self::Accept => BridgeDisposition::Accept,
            Self::Release => BridgeDisposition::Release,
            Self::Reject => BridgeDisposition::Reject,
        }
    }

    const fn from_bridge(value: BridgeDisposition) -> Self {
        match value {
            BridgeDisposition::Accept => Self::Accept,
            BridgeDisposition::Release => Self::Release,
            BridgeDisposition::Reject => Self::Reject,
        }
    }
}

/// One decision correlated to the exact acquisition behind a share record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShareRecordDecision {
    inner: BridgeDecision,
}

impl ShareRecordDecision {
    pub(super) const fn from_bridge(inner: BridgeDecision) -> Self {
        Self { inner }
    }

    pub(super) const fn into_bridge(self) -> BridgeDecision {
        self.inner
    }

    /// Returns the absolute Kafka log offset.
    pub const fn offset(self) -> i64 {
        self.inner.offset()
    }

    /// Returns the application disposition.
    pub const fn disposition(self) -> ShareDisposition {
        ShareDisposition::from_bridge(self.inner.disposition())
    }
}

/// One exact acknowledgement capability not yet admitted to transport.
///
/// Dropping this value sends nothing and returns its acquisitions locally.
#[must_use = "dropping an acknowledgement sends nothing"]
pub struct ShareAcknowledgement {
    inner: BridgeAcknowledgement,
}

impl ShareAcknowledgement {
    pub(crate) const fn from_bridge(inner: BridgeAcknowledgement) -> Self {
        Self { inner }
    }

    /// Returns the number of exact acquired ranges consumed by this capability.
    pub fn acquisition_count(&self) -> usize {
        self.inner.acquisition_count()
    }

    /// Returns the number of normalized topic-partition ranges sent to Kafka.
    pub fn range_count(&self) -> usize {
        self.inner.range_count()
    }

    pub(super) fn into_bridge(self) -> BridgeAcknowledgement {
        self.inner
    }
}

impl std::fmt::Debug for ShareAcknowledgement {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ShareAcknowledgement")
            .field("acquisition_count", &self.acquisition_count())
            .field("range_count", &self.range_count())
            .finish_non_exhaustive()
    }
}
