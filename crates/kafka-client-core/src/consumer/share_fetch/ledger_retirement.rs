//! Exact local byte release and lock retirement for share acquisitions.

use crate::{ByteCount, Moment};

use super::{
    ShareAcquisitionAdmissionErrorKind as ErrorKind, ShareAcquisitionLedger, ShareAcquisitionPhase,
    ShareAcquisitionRelease, ShareFetchSessionFence,
};

impl ShareAcquisitionLedger {
    /// Retires at most one expired range and returns its remaining local byte charge.
    pub fn expire_one(
        &mut self,
        now: Moment,
    ) -> Result<Option<ShareAcquisitionRelease>, ErrorKind> {
        let index = self.entries.iter().position(|entry| {
            entry.phase.is_locally_reclaimable() && entry.range.lock_deadline().is_elapsed_at(now)
        });
        self.retire_index(index)
    }

    /// Retires at most one range acquired under an exact lost session.
    pub fn retire_one_session(
        &mut self,
        fence: ShareFetchSessionFence,
    ) -> Result<Option<ShareAcquisitionRelease>, ErrorKind> {
        let index = self
            .entries
            .iter()
            .position(|entry| entry.fence == fence && entry.phase.is_locally_reclaimable());
        self.retire_index(index)
    }

    /// Retires one locally reclaimable range owned by this complete ledger.
    pub fn retire_one_reclaimable(&mut self) -> Result<Option<ShareAcquisitionRelease>, ErrorKind> {
        let index = self
            .entries
            .iter()
            .position(|entry| entry.phase.is_locally_reclaimable());
        self.retire_index(index)
    }

    fn retire_index(
        &mut self,
        index: Option<usize>,
    ) -> Result<Option<ShareAcquisitionRelease>, ErrorKind> {
        let Some(index) = index else {
            return Ok(None);
        };
        let entry = &self.entries[index];
        let records = self
            .retained_records
            .checked_sub(entry.range.record_count())
            .ok_or(ErrorKind::AccountingInvariant)?;
        let retained_bytes = if entry.phase == ShareAcquisitionPhase::Abandoned {
            ByteCount::new(0)
        } else {
            entry.range.retained_bytes()
        };
        let bytes = self
            .retained_bytes
            .checked_sub(retained_bytes)
            .ok_or(ErrorKind::AccountingInvariant)?;
        let generation = entry.generation;
        self.entries.remove(index);
        self.retained_records = records;
        self.retained_bytes = bytes;
        Ok(Some(ShareAcquisitionRelease::new(
            generation,
            retained_bytes,
        )))
    }
}
