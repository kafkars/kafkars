//! Atomic single-attempt ACL creation and sole terminal assignment.

use crate::DeliveryStatus;

use super::{
    CREATE_ACLS_DIAGNOSTIC_BYTES, CreateAclResult, CreateAclsBatch, CreateAclsEffect,
    CreateAclsFailure, CreateAclsFailureKind, CreateAclsInput, CreateAclsMachine,
    CreateAclsMachineError, CreateAclsRoute, CreateAclsState, CreateAclsTerminal,
    CreateAclsTransition,
};

impl CreateAclsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: CreateAclsInput,
    ) -> Result<CreateAclsTransition, CreateAclsMachineError> {
        if self.state == CreateAclsState::Completed {
            return Err(CreateAclsMachineError::AlreadyCompleted);
        }
        match input {
            CreateAclsInput::Start { now } => self.start(now),
            CreateAclsInput::DriverAccepted => self.driver_accepted(),
            CreateAclsInput::DriverRejected => self.finish_awaiting(
                CreateAclsFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            CreateAclsInput::DeadlineElapsed => self.finish_awaiting(
                CreateAclsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            CreateAclsInput::DriverDeadlineElapsed { delivery } => {
                self.finish_submitted(CreateAclsFailureKind::DeadlineElapsed, delivery)
            }
            CreateAclsInput::BrokerResponded {
                throttle_time_ms,
                results,
            } => self.broker_responded(throttle_time_ms, results),
            CreateAclsInput::ResponseTooLarge => self.finish_submitted(
                CreateAclsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            CreateAclsInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(CreateAclsFailureKind::Compatibility, delivery)
            }
            CreateAclsInput::TransportFailed { delivery } => {
                self.finish_submitted(CreateAclsFailureKind::Transport, delivery)
            }
            CreateAclsInput::InvalidResponse => self.finish_submitted(
                CreateAclsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<CreateAclsTransition, CreateAclsMachineError> {
        if self.state != CreateAclsState::Ready {
            return Err(CreateAclsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                CreateAclsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        let plan = self
            .plan
            .as_ref()
            .ok_or(CreateAclsMachineError::InvalidState)?
            .clone();
        self.state = CreateAclsState::AwaitingDriver;
        Ok(CreateAclsTransition::one(CreateAclsEffect::Submit {
            operation_id: self.operation_id,
            deadline: self.deadline,
            route: CreateAclsRoute::AnyBroker,
            plan,
        }))
    }

    fn driver_accepted(&mut self) -> Result<CreateAclsTransition, CreateAclsMachineError> {
        if self.state != CreateAclsState::AwaitingDriver {
            return Err(CreateAclsMachineError::InvalidState);
        }
        self.state = CreateAclsState::Submitted;
        Ok(CreateAclsTransition::none())
    }

    fn broker_responded(
        &mut self,
        throttle_time_ms: u32,
        results: Vec<CreateAclResult>,
    ) -> Result<CreateAclsTransition, CreateAclsMachineError> {
        if self.state != CreateAclsState::Submitted {
            return Err(CreateAclsMachineError::InvalidState);
        }
        let expected = self
            .plan
            .as_ref()
            .ok_or(CreateAclsMachineError::InvalidState)?
            .required_result_capacity();
        if results.len() != expected || results.iter().any(result_is_malformed) {
            return Ok(self.finish_failure(
                CreateAclsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        let plan = self
            .plan
            .take()
            .ok_or(CreateAclsMachineError::InvalidState)?;
        Ok(
            self.finish(CreateAclsTerminal::Created(CreateAclsBatch::from_plan(
                throttle_time_ms,
                plan,
                results,
            ))),
        )
    }

    fn finish_awaiting(
        &mut self,
        kind: CreateAclsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<CreateAclsTransition, CreateAclsMachineError> {
        if self.state != CreateAclsState::AwaitingDriver {
            return Err(CreateAclsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: CreateAclsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<CreateAclsTransition, CreateAclsMachineError> {
        if self.state != CreateAclsState::Submitted {
            return Err(CreateAclsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_failure(
        &mut self,
        kind: CreateAclsFailureKind,
        delivery: DeliveryStatus,
    ) -> CreateAclsTransition {
        self.finish(CreateAclsTerminal::Failed(CreateAclsFailure::new(
            kind, delivery,
        )))
    }

    fn finish(&mut self, terminal: CreateAclsTerminal) -> CreateAclsTransition {
        self.state = CreateAclsState::Completed;
        self.plan = None;
        CreateAclsTransition::one(CreateAclsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}

fn result_is_malformed(result: &CreateAclResult) -> bool {
    match result {
        CreateAclResult::Created => false,
        CreateAclResult::BrokerFailed(error) => error
            .message()
            .is_some_and(|message| message.len() > CREATE_ACLS_DIAGNOSTIC_BYTES),
    }
}
