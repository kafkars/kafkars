//! Atomic caller-ordered API-77 transitions, aggregation, and terminal assignment.

use core::mem::{size_of, take};

use crate::DeliveryStatus;

use super::{
    DESCRIBE_SHARE_GROUP_MAX_RESPONSE_TEXT_BYTES, DESCRIBE_SHARE_GROUP_MAX_RETAINED_BYTES,
    DescribeShareGroupBrokerError, DescribeShareGroupEffect, DescribeShareGroupFailure,
    DescribeShareGroupFailureKind, DescribeShareGroupInput, DescribeShareGroupMachine,
    DescribeShareGroupMachineError, DescribeShareGroupOutcome, DescribeShareGroupPlanShape,
    DescribeShareGroupResult, DescribeShareGroupState, DescribeShareGroupTerminal,
    DescribeShareGroupTransition, DescribeShareGroupsBatch,
    correlation::{ResponseValidation, broker_error_is_valid, canonicalize_response},
};

impl DescribeShareGroupMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: DescribeShareGroupInput,
    ) -> Result<DescribeShareGroupTransition, DescribeShareGroupMachineError> {
        if self.state == DescribeShareGroupState::Completed {
            return Err(DescribeShareGroupMachineError::AlreadyCompleted);
        }
        match input {
            DescribeShareGroupInput::Start { now } => self.start(now),
            DescribeShareGroupInput::DriverAccepted => self.driver_accepted(),
            DescribeShareGroupInput::DriverRejected => self.finish_awaiting(
                DescribeShareGroupFailureKind::DriverRejected,
                self.current_unsent_delivery(),
            ),
            DescribeShareGroupInput::DeadlineElapsed => self.finish_awaiting(
                DescribeShareGroupFailureKind::DeadlineElapsed,
                self.current_unsent_delivery(),
            ),
            DescribeShareGroupInput::DriverDeadlineElapsed { delivery } => self.finish_submitted(
                DescribeShareGroupFailureKind::DeadlineElapsed,
                self.aggregate_delivery(delivery),
            ),
            DescribeShareGroupInput::BrokerResponded { result } => self.broker_responded(result),
            DescribeShareGroupInput::BrokerRejected { error } => self.broker_rejected(error),
            DescribeShareGroupInput::ResponseTooLarge => self.finish_submitted(
                DescribeShareGroupFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            DescribeShareGroupInput::ProtocolIncompatible { delivery } => self.finish_submitted(
                DescribeShareGroupFailureKind::Compatibility,
                self.aggregate_delivery(delivery),
            ),
            DescribeShareGroupInput::TransportFailed { delivery } => self.finish_submitted(
                DescribeShareGroupFailureKind::Transport,
                self.aggregate_delivery(delivery),
            ),
            DescribeShareGroupInput::InvalidResponse => self.finish_submitted(
                DescribeShareGroupFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<DescribeShareGroupTransition, DescribeShareGroupMachineError> {
        if self.state != DescribeShareGroupState::Ready {
            return Err(DescribeShareGroupMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                DescribeShareGroupFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.submit_current()
    }

    fn submit_current(
        &mut self,
    ) -> Result<DescribeShareGroupTransition, DescribeShareGroupMachineError> {
        let Some(plan) = self.plan.singleton_at(self.next_group) else {
            return Err(DescribeShareGroupMachineError::InvalidState);
        };
        self.state = DescribeShareGroupState::AwaitingDriver;
        Ok(DescribeShareGroupTransition::one(
            DescribeShareGroupEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan,
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<DescribeShareGroupTransition, DescribeShareGroupMachineError> {
        if self.state != DescribeShareGroupState::AwaitingDriver {
            return Err(DescribeShareGroupMachineError::InvalidState);
        }
        self.state = DescribeShareGroupState::Submitted;
        Ok(DescribeShareGroupTransition::none())
    }

    fn broker_responded(
        &mut self,
        result: DescribeShareGroupResult,
    ) -> Result<DescribeShareGroupTransition, DescribeShareGroupMachineError> {
        if self.state != DescribeShareGroupState::Submitted {
            return Err(DescribeShareGroupMachineError::InvalidState);
        }
        let Some(plan) = self.plan.singleton_at(self.next_group) else {
            return Err(DescribeShareGroupMachineError::InvalidState);
        };
        match canonicalize_response(&plan, result) {
            ResponseValidation::Valid(result, text_bytes, retained_bytes) => {
                let nested_retained_bytes =
                    retained_bytes.saturating_sub(size_of::<DescribeShareGroupResult>());
                if !self.charge_response(text_bytes, nested_retained_bytes) {
                    return Ok(self.finish_failure(
                        DescribeShareGroupFailureKind::ResponseTooLarge,
                        DeliveryStatus::PossiblySent,
                    ));
                }
                self.record_outcome(DescribeShareGroupOutcome::described(result))
            }
            ResponseValidation::TooLarge => Ok(self.finish_failure(
                DescribeShareGroupFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            )),
            ResponseValidation::Invalid => Ok(self.finish_failure(
                DescribeShareGroupFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            )),
        }
    }

    fn broker_rejected(
        &mut self,
        error: DescribeShareGroupBrokerError,
    ) -> Result<DescribeShareGroupTransition, DescribeShareGroupMachineError> {
        if self.state != DescribeShareGroupState::Submitted {
            return Err(DescribeShareGroupMachineError::InvalidState);
        }
        let Some(group_id) = self.current_group_id().map(str::to_owned) else {
            return Err(DescribeShareGroupMachineError::InvalidState);
        };
        if !broker_error_is_valid(&error) {
            return Ok(self.finish_failure(
                DescribeShareGroupFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        let retained_bytes = error.message().map_or(0, str::len);
        let Some(text_bytes) = group_id.len().checked_add(retained_bytes) else {
            return Ok(self.finish_failure(
                DescribeShareGroupFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ));
        };
        if !self.charge_response(text_bytes, text_bytes) {
            return Ok(self.finish_failure(
                DescribeShareGroupFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ));
        }
        self.record_outcome(DescribeShareGroupOutcome::broker_rejected(group_id, error))
    }

    fn charge_response(&mut self, text_bytes: usize, retained_bytes: usize) -> bool {
        let Some(total_text) = self.response_text_bytes.checked_add(text_bytes) else {
            return false;
        };
        let Some(total_retained) = self.response_retained_bytes.checked_add(retained_bytes) else {
            return false;
        };
        if total_text > DESCRIBE_SHARE_GROUP_MAX_RESPONSE_TEXT_BYTES
            || total_retained > DESCRIBE_SHARE_GROUP_MAX_RETAINED_BYTES
        {
            return false;
        }
        self.response_text_bytes = total_text;
        self.response_retained_bytes = total_retained;
        true
    }

    fn record_outcome(
        &mut self,
        outcome: DescribeShareGroupOutcome,
    ) -> Result<DescribeShareGroupTransition, DescribeShareGroupMachineError> {
        let Some(expected_group_id) = self.current_group_id() else {
            return Err(DescribeShareGroupMachineError::InvalidState);
        };
        if outcome.group_id() != expected_group_id {
            return Ok(self.finish_failure(
                DescribeShareGroupFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        self.maximum_throttle_time_ms = self
            .maximum_throttle_time_ms
            .max(outcome.throttle_time_ms());
        if self.plan.shape() == DescribeShareGroupPlanShape::Singular {
            if self.next_group != 0 || self.plan.group_ids().len() != 1 {
                return Err(DescribeShareGroupMachineError::InvalidState);
            }
            self.next_group = 1;
            let terminal = match outcome {
                DescribeShareGroupOutcome::Described(result) => {
                    DescribeShareGroupTerminal::Described(result)
                }
                DescribeShareGroupOutcome::BrokerRejected { error, .. } => {
                    DescribeShareGroupTerminal::BrokerRejected(error)
                }
            };
            return Ok(self.finish(terminal));
        }
        self.outcomes.push(outcome);
        self.next_group += 1;
        if self.next_group < self.plan.group_ids().len() {
            return self.submit_current();
        }
        let batch =
            DescribeShareGroupsBatch::new(self.maximum_throttle_time_ms, take(&mut self.outcomes));
        Ok(self.finish(DescribeShareGroupTerminal::Batch(batch)))
    }

    fn finish_awaiting(
        &mut self,
        kind: DescribeShareGroupFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeShareGroupTransition, DescribeShareGroupMachineError> {
        if self.state != DescribeShareGroupState::AwaitingDriver {
            return Err(DescribeShareGroupMachineError::InvalidState);
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
        kind: DescribeShareGroupFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeShareGroupTransition, DescribeShareGroupMachineError> {
        if self.state != DescribeShareGroupState::Submitted {
            return Err(DescribeShareGroupMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_failure(
        &mut self,
        kind: DescribeShareGroupFailureKind,
        delivery: DeliveryStatus,
    ) -> DescribeShareGroupTransition {
        self.finish(DescribeShareGroupTerminal::Failed(
            DescribeShareGroupFailure::new(kind, delivery),
        ))
    }

    fn finish(&mut self, terminal: DescribeShareGroupTerminal) -> DescribeShareGroupTransition {
        self.state = DescribeShareGroupState::Completed;
        DescribeShareGroupTransition::one(DescribeShareGroupEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}
