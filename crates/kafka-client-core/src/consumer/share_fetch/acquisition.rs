//! Generation-fenced local ownership of one acquired `ShareFetch` range.

use crate::ByteCount;

use super::{ShareAcquiredRange, ShareAcquisitionGeneration, ShareFetchSessionFence};

/// Local ownership phase of one exact acquisition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShareAcquisitionPhase {
    /// The engine owns decoded bytes not yet exposed to an application batch.
    Staged,
    /// One application batch owns the exact linear acquisition capability.
    Delivered,
    /// The batch was dropped without acknowledgement and only the lock remains.
    Abandoned,
}

/// One linear generation-fenced acquisition delivered to the application.
#[must_use = "a share acquisition must be acknowledged or abandoned exactly once"]
#[derive(Debug, Eq, PartialEq)]
pub struct ShareAcquisition {
    generation: ShareAcquisitionGeneration,
    fence: ShareFetchSessionFence,
    range: ShareAcquiredRange,
}

impl ShareAcquisition {
    pub(super) const fn delivered(
        generation: ShareAcquisitionGeneration,
        fence: ShareFetchSessionFence,
        range: ShareAcquiredRange,
    ) -> Self {
        Self {
            generation,
            fence,
            range,
        }
    }

    /// Returns the nonreused local generation.
    pub const fn generation(&self) -> ShareAcquisitionGeneration {
        self.generation
    }

    /// Returns the complete session fence that acquired the range.
    pub const fn fence(&self) -> ShareFetchSessionFence {
        self.fence
    }

    /// Returns the exact acquired range.
    pub const fn range(&self) -> ShareAcquiredRange {
        self.range
    }
}

/// Ledger-owned correlation facts for one live broker lock.
#[derive(Debug, Eq, PartialEq)]
pub(super) struct ShareAcquisitionEntry {
    pub(super) generation: ShareAcquisitionGeneration,
    pub(super) fence: ShareFetchSessionFence,
    pub(super) range: ShareAcquiredRange,
    pub(super) phase: ShareAcquisitionPhase,
}

impl ShareAcquisitionEntry {
    pub(super) const fn staged(
        generation: ShareAcquisitionGeneration,
        fence: ShareFetchSessionFence,
        range: ShareAcquiredRange,
    ) -> Self {
        Self {
            generation,
            fence,
            range,
            phase: ShareAcquisitionPhase::Staged,
        }
    }

    pub(super) const fn delivery(&self) -> ShareAcquisition {
        ShareAcquisition::delivered(self.generation, self.fence, self.range)
    }
}

/// Exact local bytes released while retiring or abandoning an acquisition.
#[must_use = "released share bytes must be reclaimed exactly once"]
#[derive(Debug, Eq, PartialEq)]
pub struct ShareAcquisitionRelease {
    generation: ShareAcquisitionGeneration,
    retained_bytes: ByteCount,
}

impl ShareAcquisitionRelease {
    pub(super) const fn new(
        generation: ShareAcquisitionGeneration,
        retained_bytes: ByteCount,
    ) -> Self {
        Self {
            generation,
            retained_bytes,
        }
    }

    /// Returns the exact retired acquisition.
    pub const fn generation(&self) -> ShareAcquisitionGeneration {
        self.generation
    }

    /// Returns the local byte charge that may be reclaimed exactly once.
    pub const fn retained_bytes(&self) -> ByteCount {
        self.retained_bytes
    }
}
