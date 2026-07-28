//! Linear transaction-call recovery after the embedded driver is destroyed.

use kafka_client_core::TransactionEndOutcome;

use super::host::{TransactionLifecycleHost, TransactionLifecycleHostError};

impl TransactionLifecycleHost {
    pub(crate) fn recover_enrollment_after_driver_shutdown(&mut self) {
        self.enrollment.recover_after_driver_shutdown();
        if self.enrollment.has_fatal_terminal() {
            self.sequencing.fence();
        }
    }

    pub(in crate::transaction) fn recover_end_after_driver_shutdown(
        &mut self,
    ) -> Result<(), TransactionLifecycleHostError> {
        let Some(pending) = self.pending_end.as_mut() else {
            return Ok(());
        };
        if let Some(call) = pending.call.take() {
            call.discard_after_driver_shutdown();
        }
        if pending.ready && pending.terminal.is_none() {
            self.settle_end(TransactionEndOutcome::Fatal)?;
        }
        Ok(())
    }
}
