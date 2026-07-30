//! Terminal recovery after the unique embedded driver has been destroyed.

use kafka_client_core::{
    DeliveryStatus, TransactionInitializationInput, TransactionInitializationState,
};

use super::TransactionInitializationHost;
use crate::transaction::initialization::TransactionInitializationHostError;

impl TransactionInitializationHost {
    fn settle_recovered_raw(
        &mut self,
        index: usize,
        terminal: super::TransactionInitTerminal,
    ) -> Result<(), TransactionInitializationHostError> {
        let input = super::terminal::terminal_input(terminal.fact());
        self.operations[index].raw_terminal = Some(terminal);
        self.settle_raw(index, input)
    }

    pub(crate) fn recover_after_driver_shutdown(
        &mut self,
    ) -> Result<(), TransactionInitializationHostError> {
        self.close_admission();
        while !self.operations.is_empty() {
            if self.operations[0].raw_terminal.is_some() {
                let raw = self.operations[0]
                    .raw_terminal
                    .take()
                    .ok_or(TransactionInitializationHostError::MissingTerminal)?;
                raw.discard();
            }
            let state = self.operations[0].machine.state();
            match state {
                TransactionInitializationState::Ready => self.apply(
                    0,
                    TransactionInitializationInput::Start {
                        now: kafka_client_core::Moment::from_tick(u64::MAX),
                    },
                )?,
                TransactionInitializationState::AwaitingDriver => {
                    if let Some(call) = self.operations[0].call.take() {
                        self.apply(0, TransactionInitializationInput::DriverAccepted)?;
                        match call.recover_after_driver_shutdown() {
                            Some(terminal) => self.settle_recovered_raw(0, terminal)?,
                            None => self.apply(
                                0,
                                TransactionInitializationInput::TransportFailed {
                                    delivery: DeliveryStatus::PossiblySent,
                                },
                            )?,
                        }
                    } else {
                        self.apply(0, TransactionInitializationInput::DriverRejected)?;
                    }
                }
                TransactionInitializationState::Submitted => {
                    if let Some(call) = self.operations[0].call.take() {
                        match call.recover_after_driver_shutdown() {
                            Some(terminal) => self.settle_recovered_raw(0, terminal)?,
                            None => self.apply(
                                0,
                                TransactionInitializationInput::TransportFailed {
                                    delivery: DeliveryStatus::PossiblySent,
                                },
                            )?,
                        }
                    } else {
                        self.apply(
                            0,
                            TransactionInitializationInput::TransportFailed {
                                delivery: DeliveryStatus::PossiblySent,
                            },
                        )?;
                    }
                }
                TransactionInitializationState::Completed => {
                    self.publish_terminal(0)?;
                }
            }
        }
        for execution in &mut self.executions {
            execution.recover_after_driver_shutdown()?;
        }
        self.executions.retain(|execution| !execution.is_closed());
        Ok(())
    }
}
