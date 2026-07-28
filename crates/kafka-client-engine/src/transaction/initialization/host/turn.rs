//! One bounded nonblocking transaction-initialization host turn.

use kafka_client_core::{TransactionInitializationInput, TransactionInitializationState};

use crate::driver::DriverOwner;

use super::{TransactionInitializationHost, TransactionInitializationTurn};
use crate::transaction::initialization::TransactionInitializationHostError;

impl TransactionInitializationHost {
    pub(crate) fn turn(
        &mut self,
        now: kafka_client_core::Moment,
        driver: &DriverOwner,
    ) -> Result<TransactionInitializationTurn, TransactionInitializationHostError> {
        if let Some(error) = self.health {
            return Err(error);
        }
        if self.owner_loss_one()?
            || self.release_one_owner()?
            || self.reclaim_one()?
            || self.poll_one_call()?
        {
            return Ok(TransactionInitializationTurn::Progress);
        }
        for execution in &mut self.executions {
            if execution.turn(now, driver)?
                == crate::transaction::TransactionLifecycleTurn::Progress
            {
                return Ok(TransactionInitializationTurn::Progress);
            }
        }
        self.executions.retain(|execution| !execution.is_closed());
        let Some(index) = self.operations.iter().position(|operation| {
            operation.machine.state() == TransactionInitializationState::AwaitingDriver
                && operation.call.is_none()
        }) else {
            return Ok(TransactionInitializationTurn::Idle);
        };
        if self.operations[index].deadline.core().is_elapsed_at(now) {
            self.apply(index, TransactionInitializationInput::DeadlineElapsed)?;
            return Ok(TransactionInitializationTurn::Progress);
        }
        let request = self.operations[index]
            .request
            .as_ref()
            .ok_or(TransactionInitializationHostError::UnknownOperation)?;
        let result = crate::driver::TransactionInitCall::submit(
            driver,
            request.transactional_id(),
            request.transaction_timeout_ms(),
            self.operations[index].deadline.transport(),
        );
        match result {
            Ok(call) => {
                self.operations[index].call = Some(call);
                self.apply(index, TransactionInitializationInput::DriverAccepted)?;
            }
            Err(_rejection) => {
                self.apply(index, TransactionInitializationInput::DriverRejected)?;
            }
        }
        Ok(TransactionInitializationTurn::Progress)
    }
}
