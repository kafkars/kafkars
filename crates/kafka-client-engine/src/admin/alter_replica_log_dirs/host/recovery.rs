//! Exact accepted-call handoff and post-driver destructive recovery.

use kafka_client_core::{
    AlterReplicaLogDirAssignment, AlterReplicaLogDirsEffect, AlterReplicaLogDirsInput,
    AlterReplicaLogDirsState, DeliveryStatus, Moment, OperationId,
};

use crate::driver::AlterReplicaLogDirsCall;

use super::{AlterReplicaLogDirsHandoff, AlterReplicaLogDirsHost, AlterReplicaLogDirsHostError};

impl AlterReplicaLogDirsHost {
    pub(crate) fn accept_call(
        &mut self,
        operation_id: OperationId,
        call: AlterReplicaLogDirsCall,
    ) -> Result<(), AlterReplicaLogDirsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AlterReplicaLogDirsHostError::UnknownOperation)?;
        if self.operations[index].handoff != AlterReplicaLogDirsHandoff::HandedOff
            || self.operations[index].call.is_some()
            || self.operations[index].recovered_call.is_some()
            || self.operations[index].raw_terminal.is_some()
        {
            return Err(AlterReplicaLogDirsHostError::InvalidHandoff);
        }
        self.operations[index].call = Some(call);
        let call = self.operations[index]
            .call
            .as_ref()
            .ok_or(AlterReplicaLogDirsHostError::InvalidHandoff)?;
        if !self.operations[index].matches_call(call) {
            return Err(AlterReplicaLogDirsHostError::SubmissionMismatch);
        }
        self.apply(operation_id, AlterReplicaLogDirsInput::DriverAccepted)
    }

    pub(crate) fn reject_handoff(
        &mut self,
        operation_id: OperationId,
        broker_id: i32,
        assignments: Vec<AlterReplicaLogDirAssignment>,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Result<(), AlterReplicaLogDirsHostError> {
        let index = self
            .operation_index(operation_id)
            .ok_or(AlterReplicaLogDirsHostError::UnknownOperation)?;
        let operation = &self.operations[index];
        if operation.handoff != AlterReplicaLogDirsHandoff::HandedOff
            || operation.call.is_some()
            || operation.recovered_call.is_some()
            || operation.raw_terminal.is_some()
        {
            return Err(AlterReplicaLogDirsHostError::InvalidHandoff);
        }
        if operation.machine.state() != AlterReplicaLogDirsState::AwaitingDriver
            || !operation.matches_evidence(
                broker_id,
                &assignments,
                request_scratch_limit,
                result_limit,
            )
        {
            return Err(AlterReplicaLogDirsHostError::SubmissionMismatch);
        }
        self.apply(operation_id, AlterReplicaLogDirsInput::DriverRejected)
    }

    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), AlterReplicaLogDirsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            match (operation.machine.state(), operation.handoff) {
                (AlterReplicaLogDirsState::Ready, _) => self.apply(
                    operation_id,
                    AlterReplicaLogDirsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    AlterReplicaLogDirsState::AwaitingDriver,
                    AlterReplicaLogDirsHandoff::Untouched,
                ) => self.apply(operation_id, AlterReplicaLogDirsInput::DriverRejected)?,
                (
                    AlterReplicaLogDirsState::AwaitingDriver,
                    AlterReplicaLogDirsHandoff::HandedOff,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, AlterReplicaLogDirsInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (AlterReplicaLogDirsState::Submitted, AlterReplicaLogDirsHandoff::Submitted) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (AlterReplicaLogDirsState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(AlterReplicaLogDirsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(&mut self, index: usize) -> Result<(), AlterReplicaLogDirsHostError> {
        if self.operations[index].recovered_call.is_none()
            && let Some(call) = self.operations[index].call.take()
        {
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        let recovered = self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(AlterReplicaLogDirsHostError::InvalidHandoff)?;
        if self.operations[index].matches_recovered(recovered) {
            Ok(())
        } else {
            Err(AlterReplicaLogDirsHostError::SubmissionMismatch)
        }
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), AlterReplicaLogDirsHostError> {
        let recovered = self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(AlterReplicaLogDirsHostError::InvalidHandoff)?;
        if !self.operations[index].matches_recovered(recovered) {
            return Err(AlterReplicaLogDirsHostError::SubmissionMismatch);
        }
        let transition =
            self.operations[index]
                .machine
                .apply(AlterReplicaLogDirsInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                })?;
        let terminal = match transition.into_effect() {
            Some(AlterReplicaLogDirsEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(AlterReplicaLogDirsHostError::MissingTerminal),
        };
        self.operations[index]
            .recovered_call
            .take()
            .ok_or(AlterReplicaLogDirsHostError::InvalidHandoff)?
            .seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
