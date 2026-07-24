//! Concrete execution owner joining position policy to tracked `ListOffsets`.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerMachine, AssignedConsumerMachineError,
    AssignedConsumerTransition, Moment, PositionOwnership,
};

use crate::{
    clock::OperationDeadline,
    driver::{
        DriverOwner, PositionAdmissionFailure, PositionCompletionFailure,
        PositionResolutionRequest, TrackedPositionCalls,
    },
    protocol::consumer::ListOffsetsIsolation,
};

use super::super::position_prepare_error::PreparePositionError;

/// One linear position effect paired with its original call-boundary deadline.
#[must_use = "a prepared position resolution must be submitted or terminally settled"]
#[derive(Debug)]
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

    pub(crate) const fn fence(&self) -> kafka_client_core::PositionFence {
        self.request.fence()
    }

    /// Reconciles queued work against core's directional position ownership.
    #[allow(
        clippy::result_large_err,
        reason = "ownership errors must return the exact linear prepared lookup"
    )]
    pub(crate) fn reconcile_ownership(
        self,
        machine: &AssignedConsumerMachine,
    ) -> Result<Option<Self>, (AssignedConsumerMachineError, Self)> {
        match machine.position_ownership(self.fence()) {
            Ok(PositionOwnership::Active) => Ok(Some(self)),
            Ok(PositionOwnership::Superseded) => Ok(None),
            Err(error) => Err((error, self)),
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
    Ownership {
        error: AssignedConsumerMachineError,
        prepared: Box<PreparedPositionResolution>,
    },
}

/// Result of one preflighted attempt to hand work to the bounded call owner.
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
        let prepared = match prepared.reconcile_ownership(machine) {
            Ok(Some(prepared)) => prepared,
            Ok(None) => return Ok(PositionSubmission::Settled(None)),
            Err((error, prepared)) => {
                return Err(PositionExecutionError::Ownership {
                    error,
                    prepared: Box::new(prepared),
                });
            }
        };
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

    pub(crate) fn retained_positions(&self) -> usize {
        self.calls.retained_count()
    }

    pub(crate) fn release_position_calls_after_driver_shutdown(
        &mut self,
    ) -> Option<PositionCompletionFailure> {
        self.calls.recover_positions_after_driver_shutdown()
    }

    #[cfg(test)]
    pub(in crate::consumer) fn install_terminal_for_test(
        &mut self,
        fence: kafka_client_core::PositionFence,
        now: Moment,
    ) {
        self.calls.install_terminal_for_test(fence, now);
    }

    #[cfg(test)]
    pub(in crate::consumer) fn install_completion_failure_for_test(
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
