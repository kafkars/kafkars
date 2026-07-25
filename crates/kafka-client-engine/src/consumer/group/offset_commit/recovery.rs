//! Post-driver-shutdown recovery with every pending owner retained by the host.

use kafka_client_core::{GroupOffsetCommitInput, OperationId};

use super::host::{GroupOffsetCommitAttempt, GroupOffsetCommitHost, GroupOffsetCommitHostError};

impl GroupOffsetCommitHost {
    pub(in crate::consumer::group) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), GroupOffsetCommitHostError> {
        self.close_admission();
        if let Some(fault) = self.preparation_fault.as_ref() {
            let _diagnostic_bytes = fault.retained_bytes();
            return Err(GroupOffsetCommitHostError::Preparation);
        }
        if self.has_effect_fault() {
            return Err(GroupOffsetCommitHostError::Preparation);
        }
        if self.shutdown_recovery.is_none() {
            self.shutdown_recovery = Some(self.calls.recover_group_commits_after_driver_shutdown());
        }
        self.attach_recovered_prepared();
        self.replay_recovered_settlements()?;
        self.recover_pending_confirmation()?;
        while let Some(operation_id) = self.operations.iter().find_map(|operation| {
            matches!(
                operation.attempt,
                Some(GroupOffsetCommitAttempt::Recovery(_))
            )
            .then_some(operation.operation_id)
        }) {
            self.settle_transport_owned_failure(operation_id)?;
        }
        self.settle_queued_not_sent()?;
        while !self.operations.is_empty() {
            self.publish_terminal(0)?;
        }
        if !self.recovery_faults.is_empty()
            || self
                .shutdown_recovery
                .as_ref()
                .is_some_and(|recovery| !recovery.is_empty())
        {
            return Err(GroupOffsetCommitHostError::Settlement);
        }
        self.shutdown_recovery = None;
        self.fault = None;
        Ok(())
    }

    fn attach_recovered_prepared(&mut self) {
        while let Some(prepared) = self
            .shutdown_recovery
            .as_mut()
            .and_then(crate::driver::GroupOffsetCommitShutdownRecovery::pop_active)
        {
            let operation_id = prepared.operation_id();
            let _installed = self.install_recovery(operation_id, prepared);
        }
        if let Some(completion) = self
            .shutdown_recovery
            .as_mut()
            .and_then(crate::driver::GroupOffsetCommitShutdownRecovery::take_completion)
        {
            let (prepared, _observation) = completion.into_parts();
            let operation_id = prepared.operation_id();
            let _installed = self.install_recovery(operation_id, prepared);
        }
    }

    fn install_recovery(
        &mut self,
        operation_id: OperationId,
        prepared: crate::protocol::consumer::PreparedGroupOffsetCommit,
    ) -> Result<(), GroupOffsetCommitHostError> {
        let Some(index) = self.operation_index(operation_id) else {
            self.recovery_faults.push(prepared);
            self.fault = Some(GroupOffsetCommitHostError::UnknownOperation);
            return Err(GroupOffsetCommitHostError::UnknownOperation);
        };
        if !matches!(
            self.operations[index].attempt,
            Some(GroupOffsetCommitAttempt::HandedOff)
        ) {
            self.recovery_faults.push(prepared);
            self.fault = Some(GroupOffsetCommitHostError::Settlement);
            return Err(GroupOffsetCommitHostError::Settlement);
        }
        self.operations[index].replace_attempt(Some(GroupOffsetCommitAttempt::Recovery(prepared)));
        Ok(())
    }

    fn settle_queued_not_sent(&mut self) -> Result<(), GroupOffsetCommitHostError> {
        while let Some(index) = self.operations.iter().position(|operation| {
            matches!(operation.attempt, Some(GroupOffsetCommitAttempt::Queued(_)))
        }) {
            self.apply_terminal(
                index,
                GroupOffsetCommitInput::DriverRejected,
                super::host::GroupOffsetCommitSettlementProvenance::DefinitelyUnsent,
            )?;
            self.operations[index].replace_attempt(None);
        }
        Ok(())
    }
}
