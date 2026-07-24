//! Concrete execution owner joining position policy to tracked `ListOffsets`.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerMachine, AssignedConsumerMachineError,
    AssignedConsumerTransition, Moment,
};

use crate::{
    clock::OperationDeadline,
    driver::{
        DriverOwner, PositionAdmissionFailure, PositionCompletionFailure,
        PositionRequestPreparationError, PositionResolutionRequest, TrackedPositionCalls,
    },
    protocol::consumer::ListOffsetsIsolation,
};

/// One linear position effect paired with its original call-boundary deadline.
#[must_use = "a prepared position resolution must be submitted or terminally settled"]
pub(crate) struct PreparedPositionResolution {
    request: PositionResolutionRequest,
}

impl PreparedPositionResolution {
    pub(crate) fn new(
        effect: AssignedConsumerEffect,
        topic: String,
        isolation: ListOffsetsIsolation,
        deadline: OperationDeadline,
    ) -> Result<Self, PreparePositionError> {
        let request = PositionResolutionRequest::from_effect(effect, topic, isolation, deadline)
            .map_err(PreparePositionError::from)?;
        Ok(Self { request })
    }
}

/// Preparation rejected a non-resolution effect without changing core state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreparePositionError {
    UnexpectedEffect,
    DeadlineMismatch {
        effect: kafka_client_core::Deadline,
        operation: kafka_client_core::Deadline,
    },
}

impl From<PositionRequestPreparationError> for PreparePositionError {
    fn from(error: PositionRequestPreparationError) -> Self {
        match error {
            PositionRequestPreparationError::UnexpectedEffect => Self::UnexpectedEffect,
            PositionRequestPreparationError::DeadlineMismatch { effect, operation } => {
                Self::DeadlineMismatch { effect, operation }
            }
        }
    }
}

/// Invariant-level failure retained rather than reclassified as Kafka policy.
#[derive(Debug)]
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "direct-consumer host integration will inspect exact invariant failures"
    )
)]
pub(crate) enum PositionExecutionError {
    Completion(PositionCompletionFailure),
    Core(AssignedConsumerMachineError),
}

/// Result of one preflighted attempt to hand work to the bounded call owner.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "direct-consumer host integration will retain backpressured ownership"
    )
)]
pub(crate) enum PositionSubmission {
    Accepted,
    Backpressured(PreparedPositionResolution),
    Settled(Option<AssignedConsumerTransition>),
}

/// Bounded concrete owner of accepted and settled position lookups.
pub(crate) struct PositionResolutionExecutor {
    calls: TrackedPositionCalls,
}

impl PositionResolutionExecutor {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            calls: TrackedPositionCalls::new(capacity),
        }
    }

    pub(crate) fn submit(
        &mut self,
        driver: &DriverOwner,
        machine: &mut AssignedConsumerMachine,
        prepared: PreparedPositionResolution,
        now: Moment,
    ) -> Result<PositionSubmission, PositionExecutionError> {
        let Some(permit) = self.calls.try_reserve() else {
            return Ok(PositionSubmission::Backpressured(prepared));
        };
        match permit.submit(driver, prepared.request, now) {
            Ok(()) => Ok(PositionSubmission::Accepted),
            Err(failure) => {
                apply_admission_failure(machine, &failure).map(PositionSubmission::Settled)
            }
        }
    }

    pub(crate) fn observe_control(&mut self, effect: AssignedConsumerEffect) {
        self.calls.observe_control(effect);
    }

    pub(crate) fn poll(
        &mut self,
        machine: &mut AssignedConsumerMachine,
        now: Moment,
    ) -> Result<Option<AssignedConsumerTransition>, PositionExecutionError> {
        let terminal = match self
            .calls
            .poll_next_ready(now)
            .map_err(PositionExecutionError::Completion)?
        {
            Some(settled) => settled.terminal(),
            None => return Ok(None),
        };
        match machine.apply(terminal.core_input()) {
            Ok(transition) => {
                self.calls.discard_settled();
                Ok(Some(transition))
            }
            Err(error) if stale_terminal(terminal.fence(), &error) => {
                self.calls.discard_settled();
                Ok(None)
            }
            Err(error) => Err(PositionExecutionError::Core(error)),
        }
    }

    pub(crate) fn retained_count(&self) -> usize {
        self.calls.retained_count()
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "host recovery integration follows this explicit driver-shutdown seam"
        )
    )]
    pub(crate) fn recover_positions_after_driver_shutdown(
        &mut self,
    ) -> Option<PositionCompletionFailure> {
        self.calls.recover_positions_after_driver_shutdown()
    }

    #[cfg(test)]
    pub(super) fn install_terminal_for_test(
        &mut self,
        fence: kafka_client_core::PositionFence,
        now: Moment,
    ) {
        self.calls.install_terminal_for_test(fence, now);
    }

    #[cfg(test)]
    pub(super) fn install_completion_failure_for_test(
        &mut self,
        fence: kafka_client_core::PositionFence,
    ) {
        self.calls.install_consumed_failure_for_test(fence);
    }
}

fn apply_admission_failure(
    machine: &mut AssignedConsumerMachine,
    failure: &PositionAdmissionFailure,
) -> Result<Option<AssignedConsumerTransition>, PositionExecutionError> {
    let terminal = failure.terminal();
    match machine.apply(terminal.core_input()) {
        Ok(transition) => Ok(Some(transition)),
        Err(error) if stale_terminal(terminal.fence(), &error) => Ok(None),
        Err(error) => Err(PositionExecutionError::Core(error)),
    }
}

fn stale_terminal(
    terminal: kafka_client_core::PositionFence,
    error: &AssignedConsumerMachineError,
) -> bool {
    match error {
        AssignedConsumerMachineError::StaleAssignment { active, supplied } => {
            terminal.assignment_epoch().get() == supplied.get() && supplied.get() < active.get()
        }
        AssignedConsumerMachineError::StalePosition { active, supplied } => {
            terminal == *supplied
                && supplied.assignment_epoch() == active.assignment_epoch()
                && supplied.partition() == active.partition()
                && supplied.position_epoch().get() < active.position_epoch().get()
        }
        AssignedConsumerMachineError::PositionResolutionNotPending { fence } => terminal == *fence,
        _ => false,
    }
}
