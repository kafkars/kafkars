//! Atomic delivery and abandonment of one complete staged acquisition batch.

use core::fmt;

use crate::Moment;

use super::{
    ShareAcquisition, ShareAcquisitionAdmissionErrorKind as ErrorKind, ShareAcquisitionLedger,
    ShareAcquisitionPhase, ShareAcquisitionRelease, ShareFetchSessionFence,
};

impl ShareAcquisitionLedger {
    /// Claims every currently staged range as one all-or-nothing application batch.
    pub fn claim_batch(
        &mut self,
        fence: ShareFetchSessionFence,
        expected: usize,
        now: Moment,
    ) -> Result<Vec<ShareAcquisition>, ErrorKind> {
        if expected == 0 {
            return Err(ErrorKind::InvalidOwnership);
        }
        let staged = self
            .entries
            .iter()
            .filter(|entry| entry.fence == fence && entry.phase == ShareAcquisitionPhase::Staged)
            .count();
        if staged != expected {
            return Err(ErrorKind::InvalidOwnership);
        }
        if self.entries.iter().any(|entry| {
            entry.fence == fence
                && entry.phase == ShareAcquisitionPhase::Staged
                && entry.range.lock_deadline().is_elapsed_at(now)
        }) {
            return Err(ErrorKind::ExpiredLock);
        }
        let mut acquisitions = Vec::new();
        acquisitions
            .try_reserve_exact(expected)
            .map_err(|_error| ErrorKind::AllocationFailed)?;
        acquisitions.extend(
            self.entries
                .iter()
                .filter(|entry| {
                    entry.fence == fence && entry.phase == ShareAcquisitionPhase::Staged
                })
                .map(super::acquisition::ShareAcquisitionEntry::delivery),
        );
        for entry in &mut self.entries {
            if entry.fence == fence && entry.phase == ShareAcquisitionPhase::Staged {
                entry.phase = ShareAcquisitionPhase::Delivered;
            }
        }
        Ok(acquisitions)
    }

    /// Abandons one exact application batch without sending broker acknowledgement.
    pub fn abandon_batch(
        &mut self,
        acquisitions: Vec<ShareAcquisition>,
    ) -> Result<Vec<ShareAcquisitionRelease>, ShareAcquisitionBatchError> {
        let preflight = preflight_abandon(self, &acquisitions);
        let (indexes, releases, released_bytes) = match preflight {
            Ok(preflight) => preflight,
            Err(kind) => return Err(ShareAcquisitionBatchError::new(kind, acquisitions)),
        };
        let Some(retained_bytes) = self.retained_bytes.checked_sub(released_bytes) else {
            return Err(ShareAcquisitionBatchError::new(
                ErrorKind::AccountingInvariant,
                acquisitions,
            ));
        };
        for index in indexes {
            self.entries[index].phase = ShareAcquisitionPhase::Abandoned;
        }
        self.retained_bytes = retained_bytes;
        Ok(releases)
    }

    /// Abandons one exact engine-staged response before application transfer.
    pub fn abandon_staged_batch(
        &mut self,
        fence: ShareFetchSessionFence,
        expected: usize,
    ) -> Result<Vec<ShareAcquisitionRelease>, ErrorKind> {
        if expected == 0
            || self
                .entries
                .iter()
                .filter(|entry| {
                    entry.fence == fence && entry.phase == ShareAcquisitionPhase::Staged
                })
                .count()
                != expected
        {
            return Err(ErrorKind::InvalidOwnership);
        }
        let mut releases = Vec::new();
        releases
            .try_reserve_exact(expected)
            .map_err(|_error| ErrorKind::AllocationFailed)?;
        let mut released_bytes = crate::ByteCount::new(0);
        for entry in self
            .entries
            .iter()
            .filter(|entry| entry.fence == fence && entry.phase == ShareAcquisitionPhase::Staged)
        {
            released_bytes = released_bytes
                .checked_add(entry.range.retained_bytes())
                .ok_or(ErrorKind::AccountingInvariant)?;
            releases.push(ShareAcquisitionRelease::new(
                entry.generation,
                entry.range.retained_bytes(),
            ));
        }
        let retained_bytes = self
            .retained_bytes
            .checked_sub(released_bytes)
            .ok_or(ErrorKind::AccountingInvariant)?;
        for entry in &mut self.entries {
            if entry.fence == fence && entry.phase == ShareAcquisitionPhase::Staged {
                entry.phase = ShareAcquisitionPhase::Abandoned;
            }
        }
        self.retained_bytes = retained_bytes;
        Ok(releases)
    }

    /// Returns the earliest lock boundary not owned by an application batch.
    pub fn next_reclaimable_deadline(&self) -> Option<crate::Deadline> {
        self.entries
            .iter()
            .filter(|entry| entry.phase != ShareAcquisitionPhase::Delivered)
            .map(|entry| entry.range.lock_deadline())
            .min()
    }
}

type AbandonPreflight = (Vec<usize>, Vec<ShareAcquisitionRelease>, crate::ByteCount);

fn preflight_abandon(
    ledger: &ShareAcquisitionLedger,
    acquisitions: &[ShareAcquisition],
) -> Result<AbandonPreflight, ErrorKind> {
    if acquisitions.is_empty() {
        return Err(ErrorKind::InvalidOwnership);
    }
    let mut indexes = Vec::new();
    let mut releases = Vec::new();
    indexes
        .try_reserve_exact(acquisitions.len())
        .map_err(|_error| ErrorKind::AllocationFailed)?;
    releases
        .try_reserve_exact(acquisitions.len())
        .map_err(|_error| ErrorKind::AllocationFailed)?;
    let mut released_bytes = crate::ByteCount::new(0);
    let fence = acquisitions
        .first()
        .map(ShareAcquisition::fence)
        .ok_or(ErrorKind::InvalidOwnership)?;
    for acquisition in acquisitions {
        if acquisition.fence() != fence {
            return Err(ErrorKind::InvalidOwnership);
        }
        let index = ledger
            .entries
            .iter()
            .position(|entry| entry.generation == acquisition.generation())
            .ok_or(ErrorKind::InvalidOwnership)?;
        let entry = &ledger.entries[index];
        if indexes.contains(&index)
            || entry.fence != acquisition.fence()
            || entry.range != acquisition.range()
            || entry.phase != ShareAcquisitionPhase::Delivered
        {
            return Err(ErrorKind::InvalidOwnership);
        }
        released_bytes = released_bytes
            .checked_add(entry.range.retained_bytes())
            .ok_or(ErrorKind::AccountingInvariant)?;
        indexes.push(index);
        releases.push(ShareAcquisitionRelease::new(
            entry.generation,
            entry.range.retained_bytes(),
        ));
    }
    Ok((indexes, releases, released_bytes))
}

/// Lossless atomic-batch rejection retaining every application capability.
#[must_use = "a rejected share batch still owns every exact acquisition"]
#[derive(Debug, Eq, PartialEq)]
pub struct ShareAcquisitionBatchError {
    kind: ErrorKind,
    acquisitions: Vec<ShareAcquisition>,
}

impl ShareAcquisitionBatchError {
    const fn new(kind: ErrorKind, acquisitions: Vec<ShareAcquisition>) -> Self {
        Self { kind, acquisitions }
    }

    /// Returns the stable rejection category.
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Recovers every exact acquisition in original delivery order.
    pub fn into_acquisitions(self) -> Vec<ShareAcquisition> {
        self.acquisitions
    }
}

impl fmt::Display for ShareAcquisitionBatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "share acquisition batch rejected: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for ShareAcquisitionBatchError {}
