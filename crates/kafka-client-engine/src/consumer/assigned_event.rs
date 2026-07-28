//! Bounded claim ownership and FIFO terminal events for direct consumers.

use std::{collections::VecDeque, sync::Arc};

use kafka_client_core::{AssignedConsumerEffect, AssignedTopicPartition};

mod claim;
#[cfg(test)]
mod claim_test;
mod model;
#[cfg(test)]
mod model_test;
mod prepared;
#[cfg(test)]
mod prepared_test;

use claim::EventClaim;
pub(crate) use model::{
    AssignedConsumerEvent, AssignedConsumerEventRecovery, AssignedConsumerEventStoreBuildError,
    AssignedConsumerEventStoreError,
};
use prepared::effect_claim;

/// Sole bounded owner of active terminal claims and unobserved terminal events.
pub(crate) struct AssignedConsumerEventStore {
    capacity: usize,
    claims: Vec<EventClaim>,
    ready: VecDeque<AssignedConsumerEvent>,
}

impl AssignedConsumerEventStore {
    pub(crate) fn new(
        partition_capacity: usize,
    ) -> Result<Self, AssignedConsumerEventStoreBuildError> {
        let mut claims = Vec::new();
        let mut ready = VecDeque::new();
        claims
            .try_reserve_exact(partition_capacity)
            .map_err(|_error| AssignedConsumerEventStoreBuildError::Allocation)?;
        ready
            .try_reserve_exact(partition_capacity)
            .map_err(|_error| AssignedConsumerEventStoreBuildError::Allocation)?;
        Ok(Self {
            capacity: partition_capacity,
            claims,
            ready,
        })
    }

    pub(crate) fn observe_effect(
        &mut self,
        effect: AssignedConsumerEffect,
    ) -> Result<(), AssignedConsumerEventStoreError> {
        match effect {
            AssignedConsumerEffect::ResolvePosition { fence, .. } => {
                self.observe_start(EventClaim::Position(fence))
            }
            AssignedConsumerEffect::FetchReady { fence, .. }
            | AssignedConsumerEffect::ArmFetchThrottle { fence, .. } => {
                self.observe_start(EventClaim::Fetch(fence))
            }
            AssignedConsumerEffect::ArmPositionThrottle { fence, .. } => {
                self.require_exact(EventClaim::Position(fence))
            }
            AssignedConsumerEffect::AuthorizeFetchDelivery { fence, .. } => {
                self.require_exact(EventClaim::Fetch(fence))
            }
            AssignedConsumerEffect::Suspend { fence } => {
                self.claims.retain(|claim| !claim.is_older_than(fence));
                Ok(())
            }
            AssignedConsumerEffect::Revoke {
                assignment_epoch,
                partition,
            } => {
                self.claims.retain(|claim| {
                    claim.partition() != partition
                        || claim.position().assignment_epoch() != assignment_epoch
                });
                Ok(())
            }
            AssignedConsumerEffect::AcceptClose { .. }
            | AssignedConsumerEffect::CompleteClose { .. } => Ok(()),
            AssignedConsumerEffect::PositionResolutionFailed { .. }
            | AssignedConsumerEffect::FetchThrottleFailed { .. }
            | AssignedConsumerEffect::FetchFailed { .. } => {
                Err(AssignedConsumerEventStoreError::TransitionMismatch)
            }
        }
    }

    pub(crate) fn retain_terminal(
        &mut self,
        topic: Arc<str>,
        effect: AssignedConsumerEffect,
    ) -> Result<(), (AssignedConsumerEventStoreError, Arc<str>)> {
        let Some(claim) = terminal_claim(effect) else {
            return Err((AssignedConsumerEventStoreError::TransitionMismatch, topic));
        };
        let Some(index) = self.claims.iter().position(|present| *present == claim) else {
            let error = if self
                .claims
                .iter()
                .any(|present| present.partition() == claim.partition())
            {
                AssignedConsumerEventStoreError::ClaimMismatch
            } else {
                AssignedConsumerEventStoreError::ClaimMissing
            };
            return Err((error, topic));
        };
        let event = match effect {
            AssignedConsumerEffect::PositionResolutionFailed { fence, failure } => {
                AssignedConsumerEvent::PositionResolutionFailed {
                    topic,
                    fence,
                    failure,
                }
            }
            AssignedConsumerEffect::FetchThrottleFailed { fence, failure } => {
                AssignedConsumerEvent::FetchThrottleFailed {
                    topic,
                    fence,
                    failure,
                }
            }
            AssignedConsumerEffect::FetchFailed { fence, failure } => {
                AssignedConsumerEvent::FetchFailed {
                    topic,
                    fence,
                    failure,
                }
            }
            _ => return Err((AssignedConsumerEventStoreError::TransitionMismatch, topic)),
        };
        let _claim = self.claims.swap_remove(index);
        self.ready.push_back(event);
        Ok(())
    }

    pub(crate) fn take_event(&mut self) -> Option<AssignedConsumerEvent> {
        self.ready.pop_front()
    }

    pub(crate) fn discard_terminal(
        &mut self,
        effect: AssignedConsumerEffect,
    ) -> Result<(), AssignedConsumerEventStoreError> {
        let claim =
            terminal_claim(effect).ok_or(AssignedConsumerEventStoreError::TransitionMismatch)?;
        let index = self
            .claims
            .iter()
            .position(|present| *present == claim)
            .ok_or(AssignedConsumerEventStoreError::ClaimMissing)?;
        self.claims.swap_remove(index);
        Ok(())
    }

    pub(crate) fn retained(&self) -> (usize, usize) {
        (self.claims.len(), self.ready.len())
    }

    pub(crate) fn recover_after_driver_shutdown(&mut self) -> AssignedConsumerEventRecovery {
        let recovery = AssignedConsumerEventRecovery::new(self.claims.len(), self.ready.len());
        self.claims.clear();
        self.ready.clear();
        recovery
    }

    fn install_replacement_claims(&mut self, effects: &[AssignedConsumerEffect]) {
        self.claims.clear();
        for effect in effects {
            if let Some(claim) = effect_claim(*effect) {
                self.claims.push(claim);
            }
        }
    }

    fn install_partition_claim(
        &mut self,
        partition: AssignedTopicPartition,
        claim: Option<EventClaim>,
    ) {
        let Some(claim) = claim else {
            return;
        };
        if let Some(index) = self
            .claims
            .iter()
            .position(|present| present.partition() == partition)
        {
            self.claims[index] = claim;
        } else {
            self.claims.push(claim);
        }
    }

    fn observe_start(&mut self, next: EventClaim) -> Result<(), AssignedConsumerEventStoreError> {
        let Some(index) = self
            .claims
            .iter()
            .position(|claim| claim.partition() == next.partition())
        else {
            return Err(AssignedConsumerEventStoreError::ClaimMissing);
        };
        let current = self.claims[index];
        if current == next {
            return Ok(());
        }
        if !current.can_advance_to(next) {
            return Err(AssignedConsumerEventStoreError::ClaimMismatch);
        }
        self.claims[index] = next;
        Ok(())
    }

    fn require_exact(&self, expected: EventClaim) -> Result<(), AssignedConsumerEventStoreError> {
        self.claims
            .contains(&expected)
            .then_some(())
            .ok_or(AssignedConsumerEventStoreError::ClaimMismatch)
    }
}

const fn terminal_claim(effect: AssignedConsumerEffect) -> Option<EventClaim> {
    match effect {
        AssignedConsumerEffect::PositionResolutionFailed { fence, .. } => {
            Some(EventClaim::Position(fence))
        }
        AssignedConsumerEffect::FetchThrottleFailed { fence, .. }
        | AssignedConsumerEffect::FetchFailed { fence, .. } => Some(EventClaim::Fetch(fence)),
        _ => None,
    }
}
