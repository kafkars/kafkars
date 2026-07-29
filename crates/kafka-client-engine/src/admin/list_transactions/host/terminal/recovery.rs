//! Exact discovery or broker-attempt recovery after unique driver destruction.

use kafka_client_core::{
    AdminListTransactionsEffect, AdminListTransactionsInput, AdminListTransactionsState,
    DeliveryStatus, Moment,
};

use super::super::{
    AdminListTransactionsHandoff, AdminListTransactionsHost, AdminListTransactionsHostError,
    AdminListTransactionsSubmissionKind,
};

impl AdminListTransactionsHost {
    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), AdminListTransactionsHostError> {
        self.close_admission();
        while let Some(operation) = self.operations.first() {
            if operation.raw_terminal.is_some() {
                self.settle_raw(0)?;
                continue;
            }
            let operation_id = operation.operation_id;
            match (operation.machine.state(), operation.handoff) {
                (AdminListTransactionsState::Ready, _) => self.apply(
                    operation_id,
                    AdminListTransactionsInput::Start {
                        now: Moment::from_tick(u64::MAX),
                    },
                )?,
                (
                    AdminListTransactionsState::AwaitingDiscoveryDriver
                    | AdminListTransactionsState::AwaitingBrokerDriver,
                    AdminListTransactionsHandoff::Untouched,
                ) => self.apply(operation_id, AdminListTransactionsInput::DriverRejected)?,
                (
                    AdminListTransactionsState::AwaitingDiscoveryDriver
                    | AdminListTransactionsState::AwaitingBrokerDriver,
                    AdminListTransactionsHandoff::HandedOff,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.apply(operation_id, AdminListTransactionsInput::DriverAccepted)?;
                    self.settle_recovered_transport(0)?;
                }
                (
                    AdminListTransactionsState::DiscoverySubmitted
                    | AdminListTransactionsState::BrokerSubmitted,
                    AdminListTransactionsHandoff::Submitted,
                ) => {
                    self.retain_recovered_call(0)?;
                    self.settle_recovered_transport(0)?;
                }
                (AdminListTransactionsState::Completed, _) => self.publish_terminal(0)?,
                _ => return Err(AdminListTransactionsHostError::InvalidHandoff),
            }
        }
        Ok(())
    }

    fn retain_recovered_call(
        &mut self,
        index: usize,
    ) -> Result<(), AdminListTransactionsHostError> {
        if self.operations[index].recovered_call.is_none()
            && let Some(call) = self.operations[index].call.take()
        {
            self.operations[index].recovered_call = call.recover_after_driver_shutdown();
        }
        if self.operations[index].recovered_call.is_none() {
            return Err(AdminListTransactionsHostError::InvalidHandoff);
        }
        if recovered_matches_active_submission(&self.operations[index]) {
            Ok(())
        } else {
            Err(AdminListTransactionsHostError::SubmissionMismatch)
        }
    }

    fn settle_recovered_transport(
        &mut self,
        index: usize,
    ) -> Result<(), AdminListTransactionsHostError> {
        if !recovered_matches_active_submission(&self.operations[index]) {
            return Err(AdminListTransactionsHostError::SubmissionMismatch);
        }
        let transition =
            self.operations[index]
                .machine
                .apply(AdminListTransactionsInput::TransportFailed {
                    delivery: DeliveryStatus::PossiblySent,
                })?;
        let terminal = match transition.into_effect() {
            Some(AdminListTransactionsEffect::Complete {
                operation_id,
                terminal,
            }) if operation_id == self.operations[index].operation_id => terminal,
            _ => return Err(AdminListTransactionsHostError::MissingTerminal),
        };
        let recovered = self.operations[index]
            .recovered_call
            .take()
            .ok_or(AdminListTransactionsHostError::InvalidHandoff)?;
        recovered.seal_recovered();
        self.operations[index].terminal = Some(terminal);
        self.publish_terminal(index)
    }
}

fn recovered_matches_active_submission(
    operation: &super::super::AdminListTransactionsOperation,
) -> bool {
    let (Some(submission), Some(recovered)) = (
        operation.active_submission.as_ref(),
        operation.recovered_call.as_ref(),
    ) else {
        return false;
    };
    match submission {
        AdminListTransactionsSubmissionKind::Discovery { retained_limit } => {
            recovered.matches_discovery(*retained_limit)
        }
        AdminListTransactionsSubmissionKind::Broker {
            broker_id,
            plan,
            retained_limit,
        } => recovered.matches_broker(*broker_id, plan, *retained_limit),
    }
}
