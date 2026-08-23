//! Atomic acquisition-ledger transitions for one exact acknowledgement capability.

use crate::Moment;

use super::{
    ShareAcknowledgement, ShareAcquisition, ShareAcquisitionAdmissionErrorKind as ErrorKind,
    ShareAcquisitionLedger, ShareAcquisitionPhase, ShareAcquisitionRelease,
};

impl ShareAcquisitionLedger {
    pub(super) fn begin_acknowledgement(
        &mut self,
        acknowledgement: &ShareAcknowledgement,
        now: Moment,
    ) -> Result<(), ErrorKind> {
        let indexes = acknowledgement_indexes(
            self,
            acknowledgement.acquisitions(),
            ShareAcquisitionPhase::Delivered,
        )?;
        if indexes.iter().any(|index| {
            self.entries[*index]
                .range
                .lock_deadline()
                .is_elapsed_at(now)
        }) {
            return Err(ErrorKind::ExpiredLock);
        }
        for index in indexes {
            self.entries[index].phase = ShareAcquisitionPhase::Acknowledging;
        }
        Ok(())
    }

    pub(super) fn restore_acknowledgement(
        &mut self,
        acknowledgement: &ShareAcknowledgement,
    ) -> Result<(), ErrorKind> {
        let indexes = acknowledgement_indexes(
            self,
            acknowledgement.acquisitions(),
            ShareAcquisitionPhase::Acknowledging,
        )?;
        for index in indexes {
            self.entries[index].phase = ShareAcquisitionPhase::Delivered;
        }
        Ok(())
    }

    pub(super) fn retire_acknowledgement(
        &mut self,
        acknowledgement: &ShareAcknowledgement,
    ) -> Result<Vec<ShareAcquisitionRelease>, ErrorKind> {
        let acquisitions = acknowledgement.acquisitions();
        let indexes =
            acknowledgement_indexes(self, acquisitions, ShareAcquisitionPhase::Acknowledging)?;
        let mut records = self.retained_records;
        let mut bytes = self.retained_bytes;
        let mut releases = Vec::new();
        releases
            .try_reserve_exact(acquisitions.len())
            .map_err(|_error| ErrorKind::AllocationFailed)?;
        for (acquisition, index) in acquisitions.iter().zip(&indexes) {
            let entry = &self.entries[*index];
            records = records
                .checked_sub(entry.range.record_count())
                .ok_or(ErrorKind::AccountingInvariant)?;
            bytes = bytes
                .checked_sub(entry.range.retained_bytes())
                .ok_or(ErrorKind::AccountingInvariant)?;
            releases.push(ShareAcquisitionRelease::new(
                acquisition.generation(),
                entry.range.retained_bytes(),
            ));
        }
        let mut removal = indexes;
        removal.sort_unstable_by(|left, right| right.cmp(left));
        for index in removal {
            self.entries.remove(index);
        }
        self.retained_records = records;
        self.retained_bytes = bytes;
        Ok(releases)
    }
}

fn acknowledgement_indexes(
    ledger: &ShareAcquisitionLedger,
    acquisitions: &[ShareAcquisition],
    phase: ShareAcquisitionPhase,
) -> Result<Vec<usize>, ErrorKind> {
    if acquisitions.is_empty() {
        return Err(ErrorKind::InvalidOwnership);
    }
    let mut indexes = Vec::new();
    indexes
        .try_reserve_exact(acquisitions.len())
        .map_err(|_error| ErrorKind::AllocationFailed)?;
    for acquisition in acquisitions {
        let index = ledger
            .entries
            .iter()
            .position(|entry| entry.generation == acquisition.generation())
            .ok_or(ErrorKind::InvalidOwnership)?;
        let entry = &ledger.entries[index];
        if indexes.contains(&index)
            || entry.fence != acquisition.fence()
            || entry.range != acquisition.range()
            || entry.phase != phase
        {
            return Err(ErrorKind::InvalidOwnership);
        }
        indexes.push(index);
    }
    Ok(indexes)
}
