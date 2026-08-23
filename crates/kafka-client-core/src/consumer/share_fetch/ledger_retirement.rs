//! Exact local byte release and lock retirement for share acquisitions.

use crate::{ByteCount, Moment};

use super::{
    ShareAcquisition, ShareAcquisitionAdmissionErrorKind as ErrorKind, ShareAcquisitionLedger,
    ShareAcquisitionPhase, ShareAcquisitionRelease, ShareFetchSessionFence,
};

impl ShareAcquisitionLedger {
    /// Consumes one exact delivered capability without sending broker acknowledgement.
    pub fn abandon(
        &mut self,
        acquisition: ShareAcquisition,
    ) -> Result<ShareAcquisitionRelease, ErrorKind> {
        let (generation, fence, range) = acquisition.into_parts();
        let entry = self
            .entries
            .iter_mut()
            .find(|entry| entry.generation == generation)
            .ok_or(ErrorKind::InvalidOwnership)?;
        if entry.fence != fence
            || entry.range != range
            || entry.phase != ShareAcquisitionPhase::Delivered
        {
            return Err(ErrorKind::InvalidOwnership);
        }
        let retained_bytes = entry.range.retained_bytes();
        self.retained_bytes = self
            .retained_bytes
            .checked_sub(retained_bytes)
            .ok_or(ErrorKind::AccountingInvariant)?;
        entry.phase = ShareAcquisitionPhase::Abandoned;
        Ok(ShareAcquisitionRelease::new(generation, retained_bytes))
    }

    /// Retires at most one expired range and returns its remaining local byte charge.
    pub fn expire_one(
        &mut self,
        now: Moment,
    ) -> Result<Option<ShareAcquisitionRelease>, ErrorKind> {
        let index = self
            .entries
            .iter()
            .position(|entry| entry.range.lock_deadline().is_elapsed_at(now));
        self.retire_index(index)
    }

    /// Retires at most one range acquired under an exact lost session.
    pub fn retire_one_session(
        &mut self,
        fence: ShareFetchSessionFence,
    ) -> Result<Option<ShareAcquisitionRelease>, ErrorKind> {
        let index = self.entries.iter().position(|entry| entry.fence == fence);
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
