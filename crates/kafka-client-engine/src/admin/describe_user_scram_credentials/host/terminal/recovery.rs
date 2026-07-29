//! Exact call and user-selection recovery after the unique driver is destroyed.

use kafka_client_core::{
    DeliveryStatus, DescribeUserScramCredentialsEffect, DescribeUserScramCredentialsInput,
    DescribeUserScramCredentialsState, Moment,
};

use super::super::{
    DescribeUserScramCredentialsHandoff, DescribeUserScramCredentialsHost,
    DescribeUserScramCredentialsHostError,
};

impl DescribeUserScramCredentialsHost {
    pub(crate) fn close_admission(&mut self) {
        self.accepting = false;
    }

    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), DescribeUserScramCredentialsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            let state = operation.machine.state();
            let handoff = operation.handoff;
            match (state, handoff) {
                (DescribeUserScramCredentialsState::Ready, _) => self.apply(
                    operation_id,
                    DescribeUserScramCredentialsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    DescribeUserScramCredentialsState::AwaitingDriver,
                    DescribeUserScramCredentialsHandoff::Untouched,
                ) => {
                    self.apply(
                        operation_id,
                        DescribeUserScramCredentialsInput::DriverRejected,
                    )?;
                }
                (
                    DescribeUserScramCredentialsState::AwaitingDriver,
                    DescribeUserScramCredentialsHandoff::HandedOff,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.apply(
                        operation_id,
                        DescribeUserScramCredentialsInput::DriverAccepted,
                    )?;
                    self.settle_recovered_transport(0)?;
                }
                (
                    DescribeUserScramCredentialsState::Submitted,
                    DescribeUserScramCredentialsHandoff::Submitted,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (DescribeUserScramCredentialsState::Completed, _) => {
                    self.publish_terminal(0)?;
                }
                _ => return Err(DescribeUserScramCredentialsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(
        &mut self,
        index: usize,
    ) -> Result<(), DescribeUserScramCredentialsHostError> {
        if self.operations[index].recovered_call.is_some() {
            return Ok(());
        }
        if let Some(call) = self.operations[index].call.take() {
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        let recovered = self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(DescribeUserScramCredentialsHostError::InvalidHandoff)?;
        if self.operations[index].matches_recovered(recovered) {
            Ok(())
        } else {
            Err(DescribeUserScramCredentialsHostError::SubmissionMismatch)
        }
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), DescribeUserScramCredentialsHostError> {
        let recovered = self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(DescribeUserScramCredentialsHostError::InvalidHandoff)?;
        if !self.operations[index].matches_recovered(recovered) {
            return Err(DescribeUserScramCredentialsHostError::SubmissionMismatch);
        }
        let transition = self.operations[index].machine.apply(
            DescribeUserScramCredentialsInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
        )?;
        let terminal = match transition.into_effect() {
            Some(DescribeUserScramCredentialsEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(DescribeUserScramCredentialsHostError::MissingTerminal),
        };
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(DescribeUserScramCredentialsHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
