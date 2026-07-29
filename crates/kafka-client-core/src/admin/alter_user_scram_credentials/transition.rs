//! Atomic SCRAM credential alteration transitions and sole terminal assignment.

use std::collections::BTreeMap;

use crate::DeliveryStatus;

use super::{
    ALTER_USER_SCRAM_CREDENTIALS_DIAGNOSTIC_BYTES, AlterUserScramCredentialOutcome,
    AlterUserScramCredentialResult, AlterUserScramCredentialsBatch,
    AlterUserScramCredentialsEffect, AlterUserScramCredentialsFailure,
    AlterUserScramCredentialsFailureKind, AlterUserScramCredentialsInput,
    AlterUserScramCredentialsMachine, AlterUserScramCredentialsMachineError,
    AlterUserScramCredentialsState, AlterUserScramCredentialsTerminal,
    AlterUserScramCredentialsTransition,
};

impl AlterUserScramCredentialsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: AlterUserScramCredentialsInput,
    ) -> Result<AlterUserScramCredentialsTransition, AlterUserScramCredentialsMachineError> {
        if self.state == AlterUserScramCredentialsState::Completed {
            return Err(AlterUserScramCredentialsMachineError::AlreadyCompleted);
        }
        match input {
            AlterUserScramCredentialsInput::Start { now } => self.start(now),
            AlterUserScramCredentialsInput::DriverAccepted => self.driver_accepted(),
            AlterUserScramCredentialsInput::DriverRejected => self.finish_awaiting(
                AlterUserScramCredentialsFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            AlterUserScramCredentialsInput::DeadlineElapsed => self.finish_awaiting(
                AlterUserScramCredentialsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            AlterUserScramCredentialsInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted(
                    AlterUserScramCredentialsFailureKind::DeadlineElapsed,
                    delivery,
                ),
            AlterUserScramCredentialsInput::BrokerResponded { batch } => {
                self.broker_responded(batch)
            }
            AlterUserScramCredentialsInput::ResponseTooLarge => self.finish_submitted(
                AlterUserScramCredentialsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            AlterUserScramCredentialsInput::ProtocolIncompatible { delivery } => self
                .finish_submitted(
                    AlterUserScramCredentialsFailureKind::Compatibility,
                    delivery,
                ),
            AlterUserScramCredentialsInput::TransportFailed { delivery } => {
                self.finish_submitted(AlterUserScramCredentialsFailureKind::Transport, delivery)
            }
            AlterUserScramCredentialsInput::InvalidResponse => self.finish_submitted(
                AlterUserScramCredentialsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<AlterUserScramCredentialsTransition, AlterUserScramCredentialsMachineError> {
        if self.state != AlterUserScramCredentialsState::Ready {
            return Err(AlterUserScramCredentialsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                AlterUserScramCredentialsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = AlterUserScramCredentialsState::AwaitingDriver;
        Ok(AlterUserScramCredentialsTransition::one(
            AlterUserScramCredentialsEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan.clone(),
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<AlterUserScramCredentialsTransition, AlterUserScramCredentialsMachineError> {
        if self.state != AlterUserScramCredentialsState::AwaitingDriver {
            return Err(AlterUserScramCredentialsMachineError::InvalidState);
        }
        self.state = AlterUserScramCredentialsState::Submitted;
        Ok(AlterUserScramCredentialsTransition::none())
    }

    fn broker_responded(
        &mut self,
        batch: AlterUserScramCredentialsBatch,
    ) -> Result<AlterUserScramCredentialsTransition, AlterUserScramCredentialsMachineError> {
        if self.state != AlterUserScramCredentialsState::Submitted {
            return Err(AlterUserScramCredentialsMachineError::InvalidState);
        }
        let Some(batch) = self.correlate_batch(batch) else {
            return Ok(self.finish_failure(
                AlterUserScramCredentialsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        };
        Ok(self.finish(AlterUserScramCredentialsTerminal::Altered(batch)))
    }

    fn correlate_batch(
        &self,
        batch: AlterUserScramCredentialsBatch,
    ) -> Option<AlterUserScramCredentialsBatch> {
        let (throttle_time_ms, outcomes) = batch.into_parts();
        if outcomes.len() != self.plan.affected_users().len() {
            return None;
        }
        let mut by_user = BTreeMap::new();
        for outcome in outcomes {
            let (user, result) = outcome.into_parts();
            if user.is_empty()
                || user.len() > super::ALTER_USER_SCRAM_CREDENTIALS_MAX_USER_NAME_BYTES
                || !result_has_bounded_diagnostic(&result)
                || by_user.insert(user, result).is_some()
            {
                return None;
            }
        }
        let mut ordered = Vec::with_capacity(self.plan.affected_users().len());
        for user in self.plan.affected_users() {
            let result = by_user.remove(user)?;
            ordered.push(match result {
                AlterUserScramCredentialResult::Altered => {
                    AlterUserScramCredentialOutcome::altered(user.clone())
                }
                AlterUserScramCredentialResult::Failed(error) => {
                    AlterUserScramCredentialOutcome::failed(user.clone(), error)
                }
            });
        }
        if !by_user.is_empty() {
            return None;
        }
        Some(AlterUserScramCredentialsBatch::new(
            throttle_time_ms,
            ordered,
        ))
    }

    fn finish_awaiting(
        &mut self,
        kind: AlterUserScramCredentialsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AlterUserScramCredentialsTransition, AlterUserScramCredentialsMachineError> {
        if self.state != AlterUserScramCredentialsState::AwaitingDriver {
            return Err(AlterUserScramCredentialsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: AlterUserScramCredentialsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<AlterUserScramCredentialsTransition, AlterUserScramCredentialsMachineError> {
        if self.state != AlterUserScramCredentialsState::Submitted {
            return Err(AlterUserScramCredentialsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_failure(
        &mut self,
        kind: AlterUserScramCredentialsFailureKind,
        delivery: DeliveryStatus,
    ) -> AlterUserScramCredentialsTransition {
        self.finish(AlterUserScramCredentialsTerminal::Failed(
            AlterUserScramCredentialsFailure::new(kind, delivery),
        ))
    }

    fn finish(
        &mut self,
        terminal: AlterUserScramCredentialsTerminal,
    ) -> AlterUserScramCredentialsTransition {
        self.state = AlterUserScramCredentialsState::Completed;
        AlterUserScramCredentialsTransition::one(AlterUserScramCredentialsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}

fn result_has_bounded_diagnostic(result: &AlterUserScramCredentialResult) -> bool {
    match result {
        AlterUserScramCredentialResult::Altered => true,
        AlterUserScramCredentialResult::Failed(error) => error
            .message()
            .is_none_or(|message| message.len() <= ALTER_USER_SCRAM_CREDENTIALS_DIAGNOSTIC_BYTES),
    }
}
