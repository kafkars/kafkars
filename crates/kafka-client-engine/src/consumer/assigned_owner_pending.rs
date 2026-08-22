//! Lossless one-at-a-time admission of prepared position and Fetch work.

use kafka_client_core::{AssignedConsumerInput, Moment};

use crate::driver::DriverOwner;

use super::{
    assigned_owner::AssignedConsumerOwner,
    assigned_owner_fault::AssignedConsumerOwnerFault,
    assigned_owner_model::PendingPosition,
    fetch_execution::{FetchExecutionError, FetchSubmission},
    position_execution::PositionSubmission,
};

impl AssignedConsumerOwner {
    pub(super) fn poll_position(&mut self, now: Moment) -> bool {
        let retained = self.positions.retained_positions();
        match self.positions.poll(&mut self.machine, now) {
            Ok(Some(transition)) => {
                self.enqueue_transition(transition, None);
                true
            }
            Ok(None) => self.positions.retained_positions() < retained,
            Err(error) => {
                self.fault = Some(AssignedConsumerOwnerFault::Position(error));
                false
            }
        }
    }

    pub(super) fn drive_and_poll_fetch_executor(
        &mut self,
        driver: &DriverOwner,
        now: Moment,
    ) -> bool {
        match self
            .fetches
            .drive_broker_fetches(driver, &mut self.machine, &self.clock, now)
        {
            Ok((Some(transition), _progressed)) => {
                self.enqueue_transition(transition, None);
                return true;
            }
            Ok((None, true)) => return true,
            Ok((None, false)) => {}
            Err(error) => {
                self.fault = Some(AssignedConsumerOwnerFault::Fetch(error));
                return false;
            }
        }
        self.poll_fetch_executor_with_driver(driver, now)
    }

    fn poll_fetch_executor_with_driver(&mut self, driver: &DriverOwner, now: Moment) -> bool {
        let retained = self.fetches.retained();
        match self
            .fetches
            .poll_with_driver(driver, &mut self.machine, &mut self.events, now)
        {
            Ok(Some(transition)) => {
                self.enqueue_transition(transition, None);
                true
            }
            Ok(None) => self.fetches.retained() < retained,
            Err(error) => {
                self.fault = Some(AssignedConsumerOwnerFault::Fetch(error));
                false
            }
        }
    }

    pub(super) fn poll_fetch_executor_for_control(
        &mut self,
        driver: &DriverOwner,
        now: Moment,
    ) -> bool {
        let retained = self.fetches.retained();
        match self
            .fetches
            .poll_with_driver(driver, &mut self.machine, &mut self.events, now)
        {
            Ok(Some(transition)) => {
                self.retain_transition(transition, None);
                true
            }
            Ok(None) => self.fetches.retained() < retained,
            Err(error) => {
                self.fault = Some(AssignedConsumerOwnerFault::Fetch(error));
                true
            }
        }
    }

    pub(super) fn poll_fetch_executor(&mut self, now: Moment) -> bool {
        let retained = self.fetches.retained();
        match self.fetches.poll(&mut self.machine, now) {
            Ok(Some(transition)) => {
                self.enqueue_transition(transition, None);
                true
            }
            Ok(None) => self.fetches.retained() < retained,
            Err(error) => {
                self.fault = Some(AssignedConsumerOwnerFault::Fetch(error));
                false
            }
        }
    }

    pub(super) fn submit_position(&mut self, driver: &DriverOwner, now: Moment) -> PendingAttempt {
        let Some(pending) = self.pending_positions.pop_front() else {
            return PendingAttempt::Idle;
        };
        let pending = match pending.prepared.reconcile_ownership(&self.machine) {
            Ok(Some(prepared)) => PendingPosition {
                prepared,
                deadline: pending.deadline,
            },
            Ok(None) => return PendingAttempt::Progressed,
            Err((error, prepared)) => {
                self.fault = Some(AssignedConsumerOwnerFault::PendingPosition {
                    error,
                    pending: PendingPosition {
                        prepared,
                        deadline: pending.deadline,
                    },
                });
                return PendingAttempt::Progressed;
            }
        };
        if pending.deadline.core().is_elapsed_at(now) {
            let fence = pending.prepared.fence();
            let input = AssignedConsumerInput::PositionResolutionDeadlineElapsed { fence, now };
            match self.machine.apply(input) {
                Ok(transition) => self.enqueue_transition(transition, None),
                Err(error) => {
                    self.fault =
                        Some(AssignedConsumerOwnerFault::PendingPosition { error, pending });
                }
            }
            return PendingAttempt::Progressed;
        }
        let PendingPosition { prepared, deadline } = pending;
        match self
            .positions
            .submit(driver, &mut self.machine, prepared, now)
        {
            Ok(PositionSubmission::Accepted) => PendingAttempt::Progressed,
            Ok(PositionSubmission::Backpressured(prepared)) => {
                self.pending_positions
                    .push_front(PendingPosition { prepared, deadline });
                PendingAttempt::Backpressured
            }
            Ok(PositionSubmission::Settled(transition)) => {
                if let Some(transition) = transition {
                    self.enqueue_transition(transition, None);
                }
                PendingAttempt::Progressed
            }
            Err(error) => {
                self.fault = Some(AssignedConsumerOwnerFault::Position(error));
                PendingAttempt::Progressed
            }
        }
    }

    pub(super) fn submit_fetch(&mut self, driver: &DriverOwner, now: Moment) -> PendingAttempt {
        let Some(prepared) = self.pending_fetches.pop_front() else {
            return PendingAttempt::Idle;
        };
        match self
            .fetches
            .submit(driver, &mut self.machine, prepared, now)
        {
            Ok(FetchSubmission::Accepted) => PendingAttempt::Progressed,
            Ok(FetchSubmission::Backpressured(prepared)) => {
                self.pending_fetches.push_front(prepared);
                PendingAttempt::Backpressured
            }
            Ok(FetchSubmission::Unavailable(prepared)) => {
                self.pending_fetches.push_front(prepared);
                self.fault = Some(AssignedConsumerOwnerFault::Fetch(
                    FetchExecutionError::Faulted,
                ));
                PendingAttempt::Progressed
            }
            Ok(FetchSubmission::Settled(transition)) => {
                if let Some(transition) = transition {
                    self.enqueue_transition(transition, None);
                }
                PendingAttempt::Progressed
            }
            Err(error) => {
                self.fault = Some(AssignedConsumerOwnerFault::Fetch(error));
                PendingAttempt::Progressed
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PendingAttempt {
    Idle,
    Progressed,
    Backpressured,
}
