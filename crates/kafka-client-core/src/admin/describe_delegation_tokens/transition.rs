//! Deterministic single-submission transitions and token ordering.

use std::collections::BTreeSet;

use crate::DeliveryStatus;

use super::{
    super::DelegationToken, DescribeDelegationTokenResponse, DescribeDelegationTokensEffect,
    DescribeDelegationTokensFailure, DescribeDelegationTokensFailureKind,
    DescribeDelegationTokensInput, DescribeDelegationTokensListing,
    DescribeDelegationTokensMachine, DescribeDelegationTokensMachineError,
    DescribeDelegationTokensSelection, DescribeDelegationTokensState,
    DescribeDelegationTokensTerminal, DescribeDelegationTokensTransition,
};

impl DescribeDelegationTokensMachine {
    /// Applies one normalized fact and emits at most one mechanism effect.
    pub fn apply(
        &mut self,
        input: DescribeDelegationTokensInput,
    ) -> Result<DescribeDelegationTokensTransition, DescribeDelegationTokensMachineError> {
        if self.state == DescribeDelegationTokensState::Completed {
            return Err(DescribeDelegationTokensMachineError::AlreadyCompleted);
        }
        match input {
            DescribeDelegationTokensInput::Start { now } => self.start(now),
            DescribeDelegationTokensInput::DriverAccepted => self.driver_accepted(),
            DescribeDelegationTokensInput::DriverRejected => self.finish_awaiting(
                DescribeDelegationTokensFailureKind::DriverRejected,
                DeliveryStatus::NotSent,
            ),
            DescribeDelegationTokensInput::DeadlineElapsed => self.finish_awaiting(
                DescribeDelegationTokensFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ),
            DescribeDelegationTokensInput::DriverDeadlineElapsed { delivery } => self
                .finish_submitted(
                    DescribeDelegationTokensFailureKind::DeadlineElapsed,
                    delivery,
                ),
            DescribeDelegationTokensInput::BrokerResponded { response } => {
                self.broker_responded(response)
            }
            DescribeDelegationTokensInput::BrokerRejected { error } => self
                .finish_submitted_terminal(DescribeDelegationTokensTerminal::BrokerRejected(error)),
            DescribeDelegationTokensInput::ResponseTooLarge => self.finish_submitted(
                DescribeDelegationTokensFailureKind::ResponseTooLarge,
                DeliveryStatus::PossiblySent,
            ),
            DescribeDelegationTokensInput::ProtocolIncompatible { delivery } => {
                self.finish_submitted(DescribeDelegationTokensFailureKind::Compatibility, delivery)
            }
            DescribeDelegationTokensInput::TransportFailed { delivery } => {
                self.finish_submitted(DescribeDelegationTokensFailureKind::Transport, delivery)
            }
            DescribeDelegationTokensInput::InvalidResponse => self.finish_submitted(
                DescribeDelegationTokensFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ),
        }
    }

    fn start(
        &mut self,
        now: crate::Moment,
    ) -> Result<DescribeDelegationTokensTransition, DescribeDelegationTokensMachineError> {
        if self.state != DescribeDelegationTokensState::Ready {
            return Err(DescribeDelegationTokensMachineError::InvalidState);
        }
        if self.deadline.is_elapsed_at(now) {
            return Ok(self.finish_failure(
                DescribeDelegationTokensFailureKind::DeadlineElapsed,
                DeliveryStatus::NotSent,
            ));
        }
        self.state = DescribeDelegationTokensState::AwaitingDriver;
        Ok(DescribeDelegationTokensTransition::one(
            DescribeDelegationTokensEffect::Submit {
                operation_id: self.operation_id,
                deadline: self.deadline,
                plan: self.plan.clone(),
            },
        ))
    }

    fn driver_accepted(
        &mut self,
    ) -> Result<DescribeDelegationTokensTransition, DescribeDelegationTokensMachineError> {
        if self.state != DescribeDelegationTokensState::AwaitingDriver {
            return Err(DescribeDelegationTokensMachineError::InvalidState);
        }
        self.state = DescribeDelegationTokensState::Submitted;
        Ok(DescribeDelegationTokensTransition::none())
    }

    fn broker_responded(
        &mut self,
        response: super::DescribeDelegationTokensResponse,
    ) -> Result<DescribeDelegationTokensTransition, DescribeDelegationTokensMachineError> {
        if self.state != DescribeDelegationTokensState::Submitted {
            return Err(DescribeDelegationTokensMachineError::InvalidState);
        }
        let (throttle_time_ms, mut tokens) = response.into_parts();
        if !order_tokens(self.plan.selection(), &mut tokens) {
            return Ok(self.finish_failure(
                DescribeDelegationTokensFailureKind::InvalidResponse,
                DeliveryStatus::PossiblySent,
            ));
        }
        let tokens = tokens.into_iter().map(into_token).collect();
        Ok(self.finish(DescribeDelegationTokensTerminal::Described(
            DescribeDelegationTokensListing::new(throttle_time_ms, tokens),
        )))
    }

    fn finish_awaiting(
        &mut self,
        kind: DescribeDelegationTokensFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeDelegationTokensTransition, DescribeDelegationTokensMachineError> {
        if self.state != DescribeDelegationTokensState::AwaitingDriver {
            return Err(DescribeDelegationTokensMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted(
        &mut self,
        kind: DescribeDelegationTokensFailureKind,
        delivery: DeliveryStatus,
    ) -> Result<DescribeDelegationTokensTransition, DescribeDelegationTokensMachineError> {
        if self.state != DescribeDelegationTokensState::Submitted {
            return Err(DescribeDelegationTokensMachineError::InvalidState);
        }
        Ok(self.finish_failure(kind, delivery))
    }

    fn finish_submitted_terminal(
        &mut self,
        terminal: DescribeDelegationTokensTerminal,
    ) -> Result<DescribeDelegationTokensTransition, DescribeDelegationTokensMachineError> {
        if self.state != DescribeDelegationTokensState::Submitted {
            return Err(DescribeDelegationTokensMachineError::InvalidState);
        }
        Ok(self.finish(terminal))
    }

    fn finish_failure(
        &mut self,
        kind: DescribeDelegationTokensFailureKind,
        delivery: DeliveryStatus,
    ) -> DescribeDelegationTokensTransition {
        self.finish(DescribeDelegationTokensTerminal::Failed(
            DescribeDelegationTokensFailure::new(kind, delivery),
        ))
    }

    fn finish(
        &mut self,
        terminal: DescribeDelegationTokensTerminal,
    ) -> DescribeDelegationTokensTransition {
        self.state = DescribeDelegationTokensState::Completed;
        DescribeDelegationTokensTransition::one(DescribeDelegationTokensEffect::Complete {
            operation_id: self.operation_id,
            terminal,
        })
    }
}

fn order_tokens(
    selection: &DescribeDelegationTokensSelection,
    tokens: &mut [DescribeDelegationTokenResponse],
) -> bool {
    let mut token_ids = BTreeSet::new();
    if tokens
        .iter()
        .any(|token| !token_ids.insert(token.token_id()))
    {
        return false;
    }
    drop(token_ids);
    match selection {
        DescribeDelegationTokensSelection::All => {
            tokens.sort_by(|left, right| {
                principal_key(left)
                    .cmp(&principal_key(right))
                    .then_with(|| left.token_id().as_bytes().cmp(right.token_id().as_bytes()))
            });
            true
        }
        DescribeDelegationTokensSelection::Owners(owners) => {
            if tokens
                .iter()
                .any(|token| owner_index(owners, token.owner()).is_none())
            {
                return false;
            }
            tokens.sort_by(|left, right| {
                owner_index(owners, left.owner())
                    .cmp(&owner_index(owners, right.owner()))
                    .then_with(|| left.token_id().as_bytes().cmp(right.token_id().as_bytes()))
            });
            true
        }
    }
}

fn owner_index(
    owners: &[super::super::DelegationTokenPrincipal],
    owner: &super::super::DelegationTokenPrincipal,
) -> Option<usize> {
    owners.iter().position(|candidate| candidate == owner)
}

fn principal_key(token: &DescribeDelegationTokenResponse) -> (&[u8], &[u8]) {
    (
        token.owner().principal_type().as_bytes(),
        token.owner().principal_name().as_bytes(),
    )
}

fn into_token(token: DescribeDelegationTokenResponse) -> DelegationToken {
    let (owner, requester, renewers, issue, expiry, maximum, token_id, hmac) = token.into_parts();
    DelegationToken::new(
        owner, requester, renewers, issue, expiry, maximum, token_id, hmac,
    )
}
