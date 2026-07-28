//! Atomic SCRAM credential description transitions and sole terminal assignment.

use crate::DeliveryStatus;

use super::{
    DESCRIBE_USER_SCRAM_CREDENTIALS_DIAGNOSTIC_BYTES, DESCRIBE_USER_SCRAM_CREDENTIALS_MAX_USERS,
    DescribeUserScramCredentialsBatch, DescribeUserScramCredentialsEffect,
    DescribeUserScramCredentialsFailure, DescribeUserScramCredentialsFailureKind,
    DescribeUserScramCredentialsInput, DescribeUserScramCredentialsMachine,
    DescribeUserScramCredentialsMachineError, DescribeUserScramCredentialsState,
    DescribeUserScramCredentialsTerminal, DescribeUserScramCredentialsTransition,
    DescribeUserScramCredentialsUserOutcome, DescribeUserScramCredentialsUserResult,
};

const MAX_USER_NAME_BYTES: usize = i16::MAX as usize;

impl DescribeUserScramCredentialsMachine {
    /// Applies one normalized fact without hidden I/O, retry, or cancellation.
    pub fn apply(
        &mut self,
        input: DescribeUserScramCredentialsInput,
    ) -> Result<DescribeUserScramCredentialsTransition, DescribeUserScramCredentialsMachineError>
    {
        if self.state == DescribeUserScramCredentialsState::Completed {
            return Err(DescribeUserScramCredentialsMachineError::AlreadyCompleted);
        }
        match input {
            DescribeUserScramCredentialsInput::Start { now } => self.start(now),
            DescribeUserScramCredentialsInput::DriverAccepted => self.driver_accepted(),
            DescribeUserScramCredentialsInput::DriverRejected => self.finish_awaiting(
                DescribeUserScramCredentialsFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            DescribeUserScramCredentialsInput::DeadlineElapsed => self.finish_awaiting(
                DescribeUserScramCredentialsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            DescribeUserScramCredentialsInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted(
                    DescribeUserScramCredentialsFailureKind::DeadlineElapsed,
                    delivery,
                ),
            DescribeUserScramCredentialsInput::BrokerResponded { batch } => {
                self.broker_responded(batch)
            }
            DescribeUserScramCredentialsInput::BrokerRejected { error } => {
                if diagnostic_is_oversized(&error) {
                    return self.finish_submitted(
                        DescribeUserScramCredentialsFailureKind::InvalidResponse,
                        DeliveryStatus::PossiblySent,
                    );
                }
                self.finish_submitted(
                    DescribeUserScramCredentialsFailureKind::Broker(error),
                    DeliveryStatus::PossiblySent,
                )
            }
            DescribeUserScramCredentialsInput::ResponseTooLarge => self.finish_submitted(
                DescribeUserScramCredentialsFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            DescribeUserScramCredentialsInput::ProtocolIncompatible { delivery } => self
                .finish_submitted(
                    DescribeUserScramCredentialsFailureKind::Compatibility,
                    delivery,
                ),
            DescribeUserScramCredentialsInput::TransportFailed { delivery } => {
                self.finish_submitted(DescribeUserScramCredentialsFailureKind::Transport, delivery)
            }
            DescribeUserScramCredentialsInput::InvalidResponse => self.finish_submitted(
                DescribeUserScramCredentialsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<DescribeUserScramCredentialsTransition, DescribeUserScramCredentialsMachineError>
    {
        if self.state != DescribeUserScramCredentialsState::Ready {
            return Err(DescribeUserScramCredentialsMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                DescribeUserScramCredentialsFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = DescribeUserScramCredentialsState::AwaitingDriver;
        Ok(DescribeUserScramCredentialsTransition::one(
            DescribeUserScramCredentialsEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan.clone(),
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<DescribeUserScramCredentialsTransition, DescribeUserScramCredentialsMachineError>
    {
        if self.state != DescribeUserScramCredentialsState::AwaitingDriver {
            return Err(DescribeUserScramCredentialsMachineError::InvalidState);
        }
        self.state = DescribeUserScramCredentialsState::Submitted;
        Ok(DescribeUserScramCredentialsTransition::none())
    }

    fn broker_responded(
        &mut self,
        mut batch: DescribeUserScramCredentialsBatch,
    ) -> Result<DescribeUserScramCredentialsTransition, DescribeUserScramCredentialsMachineError>
    {
        if self.state != DescribeUserScramCredentialsState::Submitted {
            return Err(DescribeUserScramCredentialsMachineError::InvalidState);
        }
        if !normalize_and_correlate(&self.plan, &mut batch) {
            return Ok(self.finish_failure(
                DescribeUserScramCredentialsFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        Ok(self.finish(DescribeUserScramCredentialsTerminal::Described(batch)))
    }

    fn finish_awaiting(
        &mut self,
        kind: DescribeUserScramCredentialsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeUserScramCredentialsTransition, DescribeUserScramCredentialsMachineError>
    {
        if self.state != DescribeUserScramCredentialsState::AwaitingDriver {
            return Err(DescribeUserScramCredentialsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: DescribeUserScramCredentialsFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeUserScramCredentialsTransition, DescribeUserScramCredentialsMachineError>
    {
        if self.state != DescribeUserScramCredentialsState::Submitted {
            return Err(DescribeUserScramCredentialsMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_failure(
        &mut self,
        kind: DescribeUserScramCredentialsFailureKind,
        delivery: DeliveryStatus,
    ) -> DescribeUserScramCredentialsTransition {
        self.finish(DescribeUserScramCredentialsTerminal::Failed(
            DescribeUserScramCredentialsFailure::new(kind, delivery),
        ))
    }

    fn finish(
        &mut self,
        terminal: DescribeUserScramCredentialsTerminal,
    ) -> DescribeUserScramCredentialsTransition {
        self.state = DescribeUserScramCredentialsState::Completed;
        DescribeUserScramCredentialsTransition::one(DescribeUserScramCredentialsEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}

fn normalize_and_correlate(
    plan: &super::DescribeUserScramCredentialsPlan,
    batch: &mut DescribeUserScramCredentialsBatch,
) -> bool {
    let outcomes = batch.outcomes_mut();
    if outcomes.len() > DESCRIBE_USER_SCRAM_CREDENTIALS_MAX_USERS
        || outcomes.iter_mut().any(outcome_is_malformed)
    {
        return false;
    }
    match plan.users() {
        None => outcomes
            .sort_unstable_by(|left, right| left.user().as_bytes().cmp(right.user().as_bytes())),
        Some(users) => {
            outcomes.sort_unstable_by_key(|outcome| {
                users
                    .iter()
                    .position(|user| user == outcome.user())
                    .unwrap_or(usize::MAX)
            });
            if outcomes.len() != users.len()
                || outcomes
                    .iter()
                    .zip(users)
                    .any(|(outcome, user)| outcome.user() != user)
            {
                return false;
            }
        }
    }
    !outcomes
        .windows(2)
        .any(|pair| pair[0].user() == pair[1].user())
}

fn outcome_is_malformed(outcome: &mut DescribeUserScramCredentialsUserOutcome) -> bool {
    if outcome.user().is_empty() || outcome.user().len() > MAX_USER_NAME_BYTES {
        return true;
    }
    let Some(credentials) = outcome.credentials_mut() else {
        return matches!(
            outcome.result(),
            DescribeUserScramCredentialsUserResult::BrokerFailed(error)
                if diagnostic_is_oversized(error)
        );
    };
    if credentials.is_empty()
        || credentials
            .iter()
            .any(|info| info.mechanism() <= 0 || info.iterations() == 0)
    {
        return true;
    }
    credentials.sort_unstable_by_key(|info| info.mechanism());
    credentials
        .windows(2)
        .any(|pair| pair[0].mechanism() == pair[1].mechanism())
}

fn diagnostic_is_oversized(error: &super::DescribeUserScramCredentialsBrokerError) -> bool {
    error
        .message()
        .is_some_and(|message| message.len() > DESCRIBE_USER_SCRAM_CREDENTIALS_DIAGNOSTIC_BYTES)
}
