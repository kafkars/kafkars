//! Exact-call post-driver recovery for one topic-partition page owner.

use kafka_client_core::{
    DeliveryStatus, DescribeTopicPartitionsEffect, DescribeTopicPartitionsInput,
    DescribeTopicPartitionsState, Moment,
};

use super::super::{
    AdminDescribeTopicPartitionsHandoff, AdminDescribeTopicPartitionsHost,
    AdminDescribeTopicPartitionsHostError,
};

impl AdminDescribeTopicPartitionsHost {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), AdminDescribeTopicPartitionsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            match (operation.machine.state(), operation.handoff) {
                (DescribeTopicPartitionsState::Ready, _) => self.apply(
                    operation_id,
                    DescribeTopicPartitionsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    DescribeTopicPartitionsState::AwaitingDriver,
                    AdminDescribeTopicPartitionsHandoff::Untouched,
                ) => self.apply(operation_id, DescribeTopicPartitionsInput::DriverRejected)?,
                (
                    DescribeTopicPartitionsState::AwaitingDriver,
                    AdminDescribeTopicPartitionsHandoff::HandedOff,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, DescribeTopicPartitionsInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (
                    DescribeTopicPartitionsState::Submitted,
                    AdminDescribeTopicPartitionsHandoff::Submitted,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (DescribeTopicPartitionsState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(AdminDescribeTopicPartitionsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(
        &mut self,
        index: usize,
    ) -> Result<(), AdminDescribeTopicPartitionsHostError> {
        if self.operations[index].recovered_call.is_some() {
            return Ok(());
        }
        if let Some(call) = self.operations[index].call.take() {
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        self.operations[index]
            .recovered_call
            .as_ref()
            .ok_or(AdminDescribeTopicPartitionsHostError::InvalidHandoff)
            .map(|_recovered| ())
    }

    pub(super) fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), AdminDescribeTopicPartitionsHostError> {
        let transition = self.operations[index].machine.apply(
            DescribeTopicPartitionsInput::TransportFailed {
                delivery: DeliveryStatus::PossiblySent,
            },
        )?;
        let terminal = match transition.into_effect() {
            Some(DescribeTopicPartitionsEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(AdminDescribeTopicPartitionsHostError::MissingTerminal),
        };
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(AdminDescribeTopicPartitionsHostError::InvalidHandoff)?;
        recovered.seal();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}
