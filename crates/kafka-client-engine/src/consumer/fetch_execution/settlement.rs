//! Poll orchestration for live and stale tracked Fetch terminals.

use kafka_client_core::{AssignedConsumerMachine, AssignedConsumerTransition, Moment};

use crate::driver::FetchPoll;

use super::{
    executor::DirectFetchExecutor,
    fault::{FetchExecutionError, RetainedFetchFault},
};

impl DirectFetchExecutor {
    pub(crate) fn poll(
        &mut self,
        machine: &mut AssignedConsumerMachine,
        now: Moment,
    ) -> Result<Option<AssignedConsumerTransition>, FetchExecutionError> {
        if self.fault.is_some() {
            return Err(FetchExecutionError::Faulted);
        }
        let poll = match if self.broker_calls_are_active() {
            self.broker_calls.poll_fetch(now)
        } else {
            self.calls.poll_fetch(now)
        } {
            Ok(poll) => poll,
            Err(error) => {
                self.fault = Some(RetainedFetchFault::Registry);
                return Err(FetchExecutionError::Completion(error));
            }
        };
        match poll {
            FetchPoll::Idle => Ok(None),
            FetchPoll::StaleConfirmationReady { fence } => {
                if self.active_index(fence).is_some() {
                    self.fault = Some(RetainedFetchFault::Staged);
                    return Err(FetchExecutionError::UnexpectedStaleReservation { fence });
                }
                let confirmation = if self.broker_calls_are_active() {
                    self.broker_calls.confirm_stale_fetch(fence)
                } else {
                    self.calls.confirm_stale_fetch(fence)
                };
                if let Err(error) = confirmation {
                    self.fault = Some(RetainedFetchFault::Registry);
                    return Err(FetchExecutionError::ConfirmStale(error));
                }
                self.abort_stale_broker_session(fence)?;
                Ok(None)
            }
            FetchPoll::TerminalReady { fence } => {
                let Some(index) = self.active_index(fence) else {
                    self.fault = Some(RetainedFetchFault::Staged);
                    return Err(FetchExecutionError::MissingReservation { fence });
                };
                let terminal = match if self.broker_calls_are_active() {
                    self.broker_calls.begin_fetch_settlement(fence)
                } else {
                    self.calls.begin_fetch_settlement(fence)
                } {
                    Ok(terminal) => terminal,
                    Err(error) => {
                        self.fault = Some(RetainedFetchFault::Registry);
                        return Err(FetchExecutionError::Begin(error));
                    }
                };
                let active = self.take_active(index);
                let fact = self.normalize_terminal(terminal, active)?;
                self.apply_terminal(machine, fact)
            }
        }
    }
}
