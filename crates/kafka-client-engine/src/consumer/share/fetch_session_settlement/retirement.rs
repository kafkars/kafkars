//! Exact local byte release for staged, expired, and closing share deliveries.

use kafka_client_core::{Moment, ShareAcquisitionAdmissionErrorKind};

use super::super::fetch_session::ShareFetchSessionOwner;

impl ShareFetchSessionOwner {
    pub(in crate::consumer::share) const fn has_staged_delivery(&self) -> bool {
        self.staged.is_some()
    }

    pub(in crate::consumer::share) fn discard_staged_delivery(
        &mut self,
    ) -> Result<bool, ShareAcquisitionAdmissionErrorKind> {
        let Some(staged) = self.staged.as_ref() else {
            return Ok(false);
        };
        if staged.acquisitions != 0 {
            let releases = self
                .machine
                .ledger_mut()
                .abandon_staged_batch(staged.fence, staged.acquisitions)?;
            drop(releases);
        }
        let staged = self
            .staged
            .take()
            .unwrap_or_else(|| unreachable!("validated staged share delivery"));
        staged.route.accept();
        drop(staged.endpoints);
        drop(staged.partitions);
        Ok(true)
    }

    pub(in crate::consumer::share) fn expire_one_reclaimable(
        &mut self,
        now: Moment,
    ) -> Result<bool, ShareAcquisitionAdmissionErrorKind> {
        let staged_expired = self.has_staged_delivery()
            && self
                .machine
                .ledger()
                .next_reclaimable_deadline()
                .is_some_and(|deadline| deadline.is_elapsed_at(now));
        if staged_expired {
            return self.discard_staged_delivery();
        }
        Ok(self.machine.ledger_mut().expire_one(now)?.is_some())
    }

    pub(in crate::consumer::share) fn retire_one_reclaimable(
        &mut self,
    ) -> Result<bool, ShareAcquisitionAdmissionErrorKind> {
        if self.discard_staged_delivery()? {
            return Ok(true);
        }
        Ok(self
            .machine
            .ledger_mut()
            .retire_one_reclaimable()?
            .is_some())
    }
}
