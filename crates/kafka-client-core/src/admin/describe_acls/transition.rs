//! Atomic ACL-description transitions and sole terminal assignment.

use crate::DeliveryStatus;

use super::{
    DESCRIBE_ACLS_DIAGNOSTIC_BYTES, DescribeAclBinding, DescribeAclsBatch, DescribeAclsEffect,
    DescribeAclsFailure, DescribeAclsFailureKind, DescribeAclsInput, DescribeAclsMachine,
    DescribeAclsMachineError, DescribeAclsState, DescribeAclsTerminal, DescribeAclsTransition,
};

const MAX_ACL_BINDING_STRING_BYTES: usize = i16::MAX as usize;

impl DescribeAclsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: DescribeAclsInput,
    ) -> Result<DescribeAclsTransition, DescribeAclsMachineError> {
        if self.state == DescribeAclsState::Completed {
            return Err(DescribeAclsMachineError::AlreadyCompleted);
        }
        match input {
            DescribeAclsInput::Start { now } => self.start(now),
            DescribeAclsInput::DriverAccepted => self.driver_accepted(),
            DescribeAclsInput::DriverRejected => self.finish_awaiting(
                DescribeAclsFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            DescribeAclsInput::DeadlineElapsed => self.finish_awaiting(
                DescribeAclsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            DescribeAclsInput::DriverDeadlineElapsed { delivery } => {
                self.finish_submitted(DescribeAclsFailureKind::DeadlineElapsed, delivery)
            }
            DescribeAclsInput::BrokerResponded { batch } => self.broker_responded(batch),
            DescribeAclsInput::BrokerRejected { error } => {
                if error
                    .message()
                    .is_some_and(|message| message.len() > DESCRIBE_ACLS_DIAGNOSTIC_BYTES)
                {
                    return self.finish_submitted(
                        DescribeAclsFailureKind::InvalidResponse,
                        DeliveryStatus::PossiblySent,
                    );
                }
                self.finish_submitted(
                    DescribeAclsFailureKind::Broker(error),
                    DeliveryStatus::PossiblySent,
                )
            }
            DescribeAclsInput::ResponseTooLarge => self.finish_submitted(
                DescribeAclsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            DescribeAclsInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(DescribeAclsFailureKind::Compatibility, delivery)
            }
            DescribeAclsInput::TransportFailed { delivery } => {
                self.finish_submitted(DescribeAclsFailureKind::Transport, delivery)
            }
            DescribeAclsInput::InvalidResponse => self.finish_submitted(
                DescribeAclsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<DescribeAclsTransition, DescribeAclsMachineError> {
        if self.state != DescribeAclsState::Ready {
            return Err(DescribeAclsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                DescribeAclsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = DescribeAclsState::AwaitingDriver;
        Ok(DescribeAclsTransition::one(DescribeAclsEffect::Submit {
            operation_id: self.operation_id,
            deadline: self.deadline,
            plan: self.plan.clone(),
        }))
    }

    fn driver_accepted(&mut self) -> Result<DescribeAclsTransition, DescribeAclsMachineError> {
        if self.state != DescribeAclsState::AwaitingDriver {
            return Err(DescribeAclsMachineError::InvalidState);
        }
        self.state = DescribeAclsState::Submitted;
        Ok(DescribeAclsTransition::none())
    }

    fn broker_responded(
        &mut self,
        mut batch: DescribeAclsBatch,
    ) -> Result<DescribeAclsTransition, DescribeAclsMachineError> {
        if self.state != DescribeAclsState::Submitted {
            return Err(DescribeAclsMachineError::InvalidState);
        }
        if batch.bindings().iter().any(binding_is_malformed) {
            return Ok(self.finish_failure(
                DescribeAclsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        batch.sort_bindings();
        if batch.bindings().windows(2).any(|pair| pair[0] == pair[1]) {
            return Ok(self.finish_failure(
                DescribeAclsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        Ok(self.finish(DescribeAclsTerminal::Described(batch)))
    }

    fn finish_awaiting(
        &mut self,
        kind: DescribeAclsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeAclsTransition, DescribeAclsMachineError> {
        if self.state != DescribeAclsState::AwaitingDriver {
            return Err(DescribeAclsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: DescribeAclsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeAclsTransition, DescribeAclsMachineError> {
        if self.state != DescribeAclsState::Submitted {
            return Err(DescribeAclsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_failure(
        &mut self,
        kind: DescribeAclsFailureKind,
        delivery: DeliveryStatus,
    ) -> DescribeAclsTransition {
        self.finish(DescribeAclsTerminal::Failed(DescribeAclsFailure::new(
            kind, delivery,
        )))
    }

    fn finish(&mut self, terminal: DescribeAclsTerminal) -> DescribeAclsTransition {
        self.state = DescribeAclsState::Completed;
        DescribeAclsTransition::one(DescribeAclsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}

fn binding_is_malformed(binding: &DescribeAclBinding) -> bool {
    binding.resource_type() < 2
        || invalid_string(binding.resource_name())
        || binding.pattern_type() < 3
        || invalid_string(binding.principal())
        || invalid_string(binding.host())
        || binding.operation() < 2
        || binding.permission_type() < 2
}

fn invalid_string(value: &str) -> bool {
    value.is_empty() || value.len() > MAX_ACL_BINDING_STRING_BYTES
}
