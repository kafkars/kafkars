//! Atomic caller-ordered API-89 transitions, aggregation, and terminal assignment.

use core::mem::{size_of, take};

use crate::DeliveryStatus;

use super::{
    DESCRIBE_STREAMS_GROUP_MAX_RESPONSE_TEXT_BYTES, DESCRIBE_STREAMS_GROUP_MAX_RETAINED_BYTES,
    DescribeStreamsGroupBrokerError, DescribeStreamsGroupEffect, DescribeStreamsGroupFailure,
    DescribeStreamsGroupFailureKind, DescribeStreamsGroupInput, DescribeStreamsGroupMachine,
    DescribeStreamsGroupMachineError, DescribeStreamsGroupOutcome, DescribeStreamsGroupPlanShape,
    DescribeStreamsGroupResult, DescribeStreamsGroupState, DescribeStreamsGroupTerminal,
    DescribeStreamsGroupTransition, DescribeStreamsGroupsBatch,
    correlation::{ResponseValidation, broker_error_charge, canonicalize_response},
};

impl DescribeStreamsGroupMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: DescribeStreamsGroupInput,
    ) -> Result<DescribeStreamsGroupTransition, DescribeStreamsGroupMachineError> {
        if self.state == DescribeStreamsGroupState::Completed {
            return Err(DescribeStreamsGroupMachineError::AlreadyCompleted);
        }
        match input {
            DescribeStreamsGroupInput::Start { now } => self.start(now),
            DescribeStreamsGroupInput::DriverAccepted => self.driver_accepted(),
            DescribeStreamsGroupInput::DriverRejected => self.finish_awaiting(
                DescribeStreamsGroupFailureKind::DriverRejected,
                self.current_unsent_delivery(),
            ),
            DescribeStreamsGroupInput::DeadlineElapsed => self.finish_awaiting(
                DescribeStreamsGroupFailureKind::DeadlineElapsed,
                self.current_unsent_delivery(),
            ),
            DescribeStreamsGroupInput::DriverDeadlineElapsed { delivery } => self.finish_submitted(
                DescribeStreamsGroupFailureKind::DeadlineElapsed,
                self.aggregate_delivery(delivery),
            ),
            DescribeStreamsGroupInput::BrokerResponded { result } => self.broker_responded(result),
            DescribeStreamsGroupInput::BrokerRejected { error } => self.broker_rejected(error),
            DescribeStreamsGroupInput::ResponseTooLarge => self.finish_submitted(
                DescribeStreamsGroupFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            DescribeStreamsGroupInput::ProtocolIncompatible { delivery } => self.finish_submitted(
                DescribeStreamsGroupFailureKind::Compatibility,
                self.aggregate_delivery(delivery),
            ),
            DescribeStreamsGroupInput::TransportFailed { delivery } => self.finish_submitted(
                DescribeStreamsGroupFailureKind::Transport,
                self.aggregate_delivery(delivery),
            ),
            DescribeStreamsGroupInput::InvalidResponse => self.finish_submitted(
                DescribeStreamsGroupFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<DescribeStreamsGroupTransition, DescribeStreamsGroupMachineError> {
        if self.state != DescribeStreamsGroupState::Ready {
            return Err(DescribeStreamsGroupMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                DescribeStreamsGroupFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.submit_current()
    }

    fn submit_current(
        &mut self,
    ) -> Result<DescribeStreamsGroupTransition, DescribeStreamsGroupMachineError> {
        let Some(plan) = self.plan.singleton_at(self.next_group) else {
            return Err(DescribeStreamsGroupMachineError::InvalidState);
        };
        self.state = DescribeStreamsGroupState::AwaitingDriver;
        Ok(DescribeStreamsGroupTransition::one(
            DescribeStreamsGroupEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan,
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<DescribeStreamsGroupTransition, DescribeStreamsGroupMachineError> {
        if self.state != DescribeStreamsGroupState::AwaitingDriver {
            return Err(DescribeStreamsGroupMachineError::InvalidState);
        }
        self.state = DescribeStreamsGroupState::Submitted;
        Ok(DescribeStreamsGroupTransition::none())
    }

    fn broker_responded(
        &mut self,
        result: DescribeStreamsGroupResult,
    ) -> Result<DescribeStreamsGroupTransition, DescribeStreamsGroupMachineError> {
        if self.state != DescribeStreamsGroupState::Submitted {
            return Err(DescribeStreamsGroupMachineError::InvalidState);
        }
        let Some(plan) = self.plan.singleton_at(self.next_group) else {
            return Err(DescribeStreamsGroupMachineError::InvalidState);
        };
        match canonicalize_response(&plan, result) {
            ResponseValidation::Valid(canonical) => {
                let nested_retained_bytes = canonical
                    .retained_bytes
                    .saturating_sub(size_of::<DescribeStreamsGroupResult>());
                if !self.charge_response(canonical.text_bytes, nested_retained_bytes) {
                    return Ok(self.finish_failure(
                        DescribeStreamsGroupFailureKind::ResponseTooLarge,
                        DeliveryStatus::PossiblySent,
                    ));
                }
                self.record_outcome(DescribeStreamsGroupOutcome::described(canonical.result))
            }
            ResponseValidation::TooLarge => Ok(self.finish_failure(
                DescribeStreamsGroupFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            )),
            ResponseValidation::Invalid => Ok(self.finish_failure(
                DescribeStreamsGroupFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            )),
        }
    }

    fn broker_rejected(
        &mut self,
        error: DescribeStreamsGroupBrokerError,
    ) -> Result<DescribeStreamsGroupTransition, DescribeStreamsGroupMachineError> {
        if self.state != DescribeStreamsGroupState::Submitted {
            return Err(DescribeStreamsGroupMachineError::InvalidState);
        }
        let Some(group_id) = self.current_group_id().map(str::to_owned) else {
            return Err(DescribeStreamsGroupMachineError::InvalidState);
        };
        let Some((text_bytes, retained_bytes)) = broker_error_charge(&group_id, &error) else {
            return Ok(self.finish_failure(
                DescribeStreamsGroupFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        };
        if !self.charge_response(text_bytes, retained_bytes) {
            return Ok(self.finish_failure(
                DescribeStreamsGroupFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ));
        }
        self.record_outcome(DescribeStreamsGroupOutcome::broker_rejected(
            group_id, error,
        ))
    }

    fn charge_response(&mut self, text_bytes: usize, retained_bytes: usize) -> bool {
        let Some(total_text) = self.response_text_bytes.checked_add(text_bytes) else {
            return false;
        };
        let Some(total_retained) = self.response_retained_bytes.checked_add(retained_bytes) else {
            return false;
        };
        if total_text > DESCRIBE_STREAMS_GROUP_MAX_RESPONSE_TEXT_BYTES
            || total_retained > DESCRIBE_STREAMS_GROUP_MAX_RETAINED_BYTES
        {
            return false;
        }
        self.response_text_bytes = total_text;
        self.response_retained_bytes = total_retained;
        true
    }

    fn record_outcome(
        &mut self,
        outcome: DescribeStreamsGroupOutcome,
    ) -> Result<DescribeStreamsGroupTransition, DescribeStreamsGroupMachineError> {
        let Some(expected_group_id) = self.current_group_id() else {
            return Err(DescribeStreamsGroupMachineError::InvalidState);
        };
        if outcome.group_id() != expected_group_id {
            return Ok(self.finish_failure(
                DescribeStreamsGroupFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        self.maximum_throttle_time_ms = self
            .maximum_throttle_time_ms
            .max(outcome.throttle_time_ms());
        self.outcomes.push(outcome);
        self.next_group += 1;
        if self.next_group < self.plan.group_ids().len() {
            return self.submit_current();
        }
        self.finish_outcomes()
    }

    fn finish_outcomes(
        &mut self,
    ) -> Result<DescribeStreamsGroupTransition, DescribeStreamsGroupMachineError> {
        let terminal = match self.plan.shape() {
            DescribeStreamsGroupPlanShape::Singular => {
                if self.outcomes.len() != 1 {
                    return Err(DescribeStreamsGroupMachineError::InvalidState);
                }
                let outcome = self
                    .outcomes
                    .pop()
                    .ok_or(DescribeStreamsGroupMachineError::InvalidState)?;
                match outcome {
                    DescribeStreamsGroupOutcome::Described(result) => {
                        DescribeStreamsGroupTerminal::Described(result)
                    }
                    DescribeStreamsGroupOutcome::BrokerRejected { error, .. } => {
                        DescribeStreamsGroupTerminal::BrokerRejected(error)
                    }
                }
            }
            DescribeStreamsGroupPlanShape::Batch => {
                DescribeStreamsGroupTerminal::Batch(DescribeStreamsGroupsBatch::new(
                    self.maximum_throttle_time_ms,
                    take(&mut self.outcomes),
                ))
            }
        };
        Ok(self.finish(terminal))
    }

    fn finish_awaiting(
        &mut self,
        kind: DescribeStreamsGroupFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeStreamsGroupTransition, DescribeStreamsGroupMachineError> {
        if self.state != DescribeStreamsGroupState::AwaitingDriver {
            return Err(DescribeStreamsGroupMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    const fn current_unsent_delivery(&self) -> DeliveryStatus {
        if self.next_group == 0 {
            DeliveryStatus::NotSent
        } else {
            DeliveryStatus::PossiblySent
        }
    }

    const fn aggregate_delivery(&self, current: DeliveryStatus) -> DeliveryStatus {
        if self.next_group == 0 {
            current
        } else {
            DeliveryStatus::PossiblySent
        }
    }

    fn finish_submitted(
        &mut self,
        kind: DescribeStreamsGroupFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeStreamsGroupTransition, DescribeStreamsGroupMachineError> {
        if self.state != DescribeStreamsGroupState::Submitted {
            return Err(DescribeStreamsGroupMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_failure(
        &mut self,
        kind: DescribeStreamsGroupFailureKind,
        delivery: DeliveryStatus,
    ) -> DescribeStreamsGroupTransition {
        self.finish(DescribeStreamsGroupTerminal::Failed(
            DescribeStreamsGroupFailure::new(kind, delivery),
        ))
    }

    fn finish(&mut self, terminal: DescribeStreamsGroupTerminal) -> DescribeStreamsGroupTransition {
        self.state = DescribeStreamsGroupState::Completed;
        DescribeStreamsGroupTransition::one(DescribeStreamsGroupEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
