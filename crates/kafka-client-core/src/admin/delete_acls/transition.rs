//! Atomic single-attempt ACL deletion and sole terminal assignment.

use crate::DeliveryStatus;

use super::{
    DELETE_ACLS_DIAGNOSTIC_BYTES, DeleteAclFilterResult, DeleteAclMatchResult,
    DeleteAclMatchingBinding, DeleteAclsBatch, DeleteAclsEffect, DeleteAclsFailure,
    DeleteAclsFailureKind, DeleteAclsInput, DeleteAclsMachine, DeleteAclsMachineError,
    DeleteAclsRoute, DeleteAclsState, DeleteAclsTerminal, DeleteAclsTransition,
    MAX_DELETE_ACLS_MATCHING_BINDINGS,
};

const MAX_ACL_BINDING_STRING_BYTES: usize = i16::MAX as usize;

impl DeleteAclsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: DeleteAclsInput,
    ) -> Result<DeleteAclsTransition, DeleteAclsMachineError> {
        if self.state == DeleteAclsState::Completed {
            return Err(DeleteAclsMachineError::AlreadyCompleted);
        }
        match input {
            DeleteAclsInput::Start { now } => self.start(now),
            DeleteAclsInput::DriverAccepted => self.driver_accepted(),
            DeleteAclsInput::DriverRejected => self.finish_awaiting(
                DeleteAclsFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            DeleteAclsInput::DeadlineElapsed => self.finish_awaiting(
                DeleteAclsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            DeleteAclsInput::DriverDeadlineElapsed { delivery } => {
                self.finish_submitted(DeleteAclsFailureKind::DeadlineElapsed, delivery)
            }
            DeleteAclsInput::BrokerResponded {
                throttle_time_ms,
                results,
            } => self.broker_responded(throttle_time_ms, results),
            DeleteAclsInput::ResponseTooLarge => self.finish_submitted(
                DeleteAclsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            DeleteAclsInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(DeleteAclsFailureKind::Compatibility, delivery)
            }
            DeleteAclsInput::TransportFailed { delivery } => {
                self.finish_submitted(DeleteAclsFailureKind::Transport, delivery)
            }
            DeleteAclsInput::InvalidResponse => self.finish_submitted(
                DeleteAclsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<DeleteAclsTransition, DeleteAclsMachineError> {
        if self.state != DeleteAclsState::Ready {
            return Err(DeleteAclsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                DeleteAclsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        let plan = self
            .plan
            .as_ref()
            .ok_or(DeleteAclsMachineError::InvalidState)?
            .clone();
        self.state = DeleteAclsState::AwaitingDriver;
        Ok(DeleteAclsTransition::one(DeleteAclsEffect::Submit {
            operation_id: self.operation_id,
            deadline: self.deadline,
            route: DeleteAclsRoute::AnyBroker,
            plan,
        }))
    }

    fn driver_accepted(&mut self) -> Result<DeleteAclsTransition, DeleteAclsMachineError> {
        if self.state != DeleteAclsState::AwaitingDriver {
            return Err(DeleteAclsMachineError::InvalidState);
        }
        self.state = DeleteAclsState::Submitted;
        Ok(DeleteAclsTransition::none())
    }

    fn broker_responded(
        &mut self,
        throttle_time_ms: u32,
        mut results: Vec<DeleteAclFilterResult>,
    ) -> Result<DeleteAclsTransition, DeleteAclsMachineError> {
        if self.state != DeleteAclsState::Submitted {
            return Err(DeleteAclsMachineError::InvalidState);
        }
        let expected = self
            .plan
            .as_ref()
            .ok_or(DeleteAclsMachineError::InvalidState)?
            .required_filter_result_capacity();
        if results.len() != expected || !validate_results(&mut results) {
            return Ok(self.finish_failure(
                DeleteAclsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        let plan = self
            .plan
            .take()
            .ok_or(DeleteAclsMachineError::InvalidState)?;
        Ok(
            self.finish(DeleteAclsTerminal::Deleted(DeleteAclsBatch::from_plan(
                throttle_time_ms,
                plan,
                results,
            ))),
        )
    }

    fn finish_awaiting(
        &mut self,
        kind: DeleteAclsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DeleteAclsTransition, DeleteAclsMachineError> {
        if self.state != DeleteAclsState::AwaitingDriver {
            return Err(DeleteAclsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: DeleteAclsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DeleteAclsTransition, DeleteAclsMachineError> {
        if self.state != DeleteAclsState::Submitted {
            return Err(DeleteAclsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_failure(
        &mut self,
        kind: DeleteAclsFailureKind,
        delivery: DeliveryStatus,
    ) -> DeleteAclsTransition {
        self.finish(DeleteAclsTerminal::Failed(DeleteAclsFailure::new(
            kind, delivery,
        )))
    }

    fn finish(&mut self, terminal: DeleteAclsTerminal) -> DeleteAclsTransition {
        self.state = DeleteAclsState::Completed;
        self.plan = None;
        DeleteAclsTransition::one(DeleteAclsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}

fn validate_results(results: &mut [DeleteAclFilterResult]) -> bool {
    let mut total_matches = 0usize;
    for result in results {
        match result {
            DeleteAclFilterResult::BrokerFailed(error) => {
                if diagnostic_is_oversized(error.message()) {
                    return false;
                }
            }
            DeleteAclFilterResult::Matched(bindings) => {
                let Some(total) = total_matches.checked_add(bindings.len()) else {
                    return false;
                };
                total_matches = total;
                if total_matches > MAX_DELETE_ACLS_MATCHING_BINDINGS
                    || !validate_matching_bindings(bindings)
                {
                    return false;
                }
            }
        }
    }
    true
}

fn validate_matching_bindings(bindings: &mut [DeleteAclMatchingBinding]) -> bool {
    for (index, binding) in bindings.iter_mut().enumerate() {
        if binding_is_malformed(binding) {
            return false;
        }
        binding.assign_response_index(index);
    }
    bindings.sort_unstable_by(DeleteAclMatchingBinding::identity_cmp);
    if bindings
        .windows(2)
        .any(|pair| pair[0].same_identity(&pair[1]))
    {
        return false;
    }
    bindings.sort_unstable_by_key(DeleteAclMatchingBinding::response_index);
    for binding in bindings {
        binding.clear_response_index();
    }
    true
}

fn binding_is_malformed(binding: &DeleteAclMatchingBinding) -> bool {
    binding.resource_type() < 2
        || invalid_string(binding.resource_name())
        || binding.pattern_type() < 3
        || invalid_string(binding.principal())
        || invalid_string(binding.host())
        || binding.operation() < 2
        || binding.permission_type() < 2
        || matches!(
            binding.result(),
            DeleteAclMatchResult::BrokerFailed(error)
                if diagnostic_is_oversized(error.message())
        )
}

fn invalid_string(value: &str) -> bool {
    value.is_empty() || value.len() > MAX_ACL_BINDING_STRING_BYTES
}

fn diagnostic_is_oversized(message: Option<&str>) -> bool {
    message.is_some_and(|message| message.len() > DELETE_ACLS_DIAGNOSTIC_BYTES)
}
