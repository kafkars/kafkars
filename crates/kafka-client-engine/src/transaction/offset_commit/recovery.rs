//! Exact local and driver-owned reconciliation after driver destruction.

use kafka_client_core::DeliveryStatus;

use crate::transaction::TransactionLifecycleHost;

use super::{
    model::{
        TransactionOffsetCommitFailure, TransactionOffsetCommitFailureKind,
        TransactionOffsetCommitHostError,
    },
    owner::TransactionOffsetCommitOwner,
    turn::{TransactionOffsetCommitSlot, TransactionOffsetCommitTurn},
};

impl TransactionOffsetCommitOwner {
    pub(in crate::transaction) fn recover_after_driver_shutdown(
        &mut self,
        lifecycle: &mut TransactionLifecycleHost,
    ) -> Result<TransactionOffsetCommitTurn, TransactionOffsetCommitHostError> {
        let slot = core::mem::replace(&mut self.slot, TransactionOffsetCommitSlot::Vacant);
        match slot {
            TransactionOffsetCommitSlot::Ready(pending, stage) => {
                self.reject_ready(
                    pending,
                    stage,
                    TransactionOffsetCommitFailure::new(
                        TransactionOffsetCommitFailureKind::DriverShutdown,
                        DeliveryStatus::NotSent,
                    ),
                )?;
            }
            TransactionOffsetCommitSlot::Calling(pending, stage, call) => {
                self.settle_evidence(
                    pending,
                    stage,
                    call.recover_after_driver_shutdown(),
                    lifecycle,
                    None,
                )?;
            }
            TransactionOffsetCommitSlot::Settling(pending, stage, evidence) => {
                self.settle_evidence(pending, stage, evidence, lifecycle, None)?;
            }
            TransactionOffsetCommitSlot::Vacant
            | TransactionOffsetCommitSlot::Terminal(_, _)
            | TransactionOffsetCommitSlot::Published => self.slot = slot,
        }
        Ok(TransactionOffsetCommitTurn::Progress)
    }
}
