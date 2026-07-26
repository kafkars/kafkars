//! Sole bounded state owner for group Fetch activation and later execution.

use std::{collections::VecDeque, time::Duration};

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerMachine, GroupPositionFence, ReadIsolation,
};

use crate::{
    consumer::{
        assigned_event::AssignedConsumerEventStore,
        assigned_timers::AssignedTimers,
        fetch_execution::{DirectFetchExecutor, PreparedFetchExecution},
    },
    protocol::{
        consumer::CLASSIC_SYNC_MAX_MEMBER_PARTITIONS,
        fetch::{FetchDecodeLimits, FetchIsolation, FetchRequestSettings},
    },
};

use super::{
    super::classic_group_position::{
        ClassicGroupPositionCompleted, prepare_classic_group_fetch_activation,
    },
    activation::{
        ClassicGroupFetchActivation, ClassicGroupFetchActivationError,
        ClassicGroupFetchActivationFailure, ClassicGroupFetchActivationFault,
        ClassicGroupFetchBinding, ClassicGroupFetchPostCoreFaultKind,
    },
    model::{
        ClassicGroupFetchBuildError, ClassicGroupFetchOwnerFault, ClassicGroupFetchPreflightError,
    },
};

/// First private slice mirrors the direct consumer's current partition bound.
pub(super) const FIRST_GROUP_FETCH_PARTITIONS: usize = CLASSIC_SYNC_MAX_MEMBER_PARTITIONS;
/// First private slice reserves replacement revokes, starts, and one close effect.
pub(super) const FIRST_GROUP_FETCH_EFFECTS: usize = FIRST_GROUP_FETCH_PARTITIONS * 2 + 1;
/// First private slice admits one group-owned Fetch call at a time.
pub(super) const FIRST_GROUP_FETCH_CALLS: usize = 1;
/// First private slice retains one group-owned delivery at a time.
pub(super) const FIRST_GROUP_FETCH_DELIVERIES: usize = 1;
/// First private slice mirrors the direct consumer's one-MiB delivery bound.
pub(super) const FIRST_GROUP_FETCH_DELIVERY_BYTES: usize = 1024 * 1024;
/// First private slice mirrors the direct consumer's one-MiB per-Fetch output bound.
pub(super) const FIRST_GROUP_FETCH_OUTPUT_BYTES: usize = 1024 * 1024;

const FIRST_GROUP_FETCH_REQUEST_BYTES: u32 = 1024 * 1024;
const FIRST_GROUP_FETCH_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);

/// One group-specific deterministic Fetch policy owner.
pub(in crate::consumer::group) struct ClassicGroupFetchOwner {
    pub(super) machine: AssignedConsumerMachine,
    activation: Option<ClassicGroupFetchActivation>,
    pub(super) timers: AssignedTimers,
    pub(super) fetches: DirectFetchExecutor,
    pub(super) events: AssignedConsumerEventStore,
    pub(super) effects: VecDeque<AssignedConsumerEffect>,
    pub(super) pending_fetches: VecDeque<PreparedFetchExecution>,
    pub(super) fetch_settings: FetchRequestSettings,
    pub(super) fetch_decode_limits: FetchDecodeLimits,
    pub(super) fetch_attempt_timeout: Duration,
    pub(super) partition_capacity: usize,
    pub(super) effect_capacity: usize,
    pub(super) hard_fetch_output_bytes: usize,
    pub(super) fault: Option<ClassicGroupFetchOwnerFault>,
}

impl ClassicGroupFetchOwner {
    pub(in crate::consumer::group) fn try_new() -> Result<Self, ClassicGroupFetchBuildError> {
        let mut effects = VecDeque::new();
        let mut pending_fetches = VecDeque::new();
        effects
            .try_reserve_exact(FIRST_GROUP_FETCH_EFFECTS)
            .map_err(|_error| ClassicGroupFetchBuildError::Allocation)?;
        pending_fetches
            .try_reserve_exact(FIRST_GROUP_FETCH_PARTITIONS)
            .map_err(|_error| ClassicGroupFetchBuildError::Allocation)?;
        let events = AssignedConsumerEventStore::new(FIRST_GROUP_FETCH_PARTITIONS)
            .map_err(|_error| ClassicGroupFetchBuildError::Allocation)?;
        Ok(Self {
            machine: AssignedConsumerMachine::with_read_isolation(ReadIsolation::ReadUncommitted),
            activation: None,
            timers: AssignedTimers::new(FIRST_GROUP_FETCH_PARTITIONS),
            fetches: DirectFetchExecutor::create_unbound(
                FIRST_GROUP_FETCH_CALLS,
                FIRST_GROUP_FETCH_DELIVERIES,
                FIRST_GROUP_FETCH_DELIVERY_BYTES,
            ),
            events,
            effects,
            pending_fetches,
            fetch_settings: FetchRequestSettings::new(
                500,
                1,
                FIRST_GROUP_FETCH_REQUEST_BYTES,
                FIRST_GROUP_FETCH_REQUEST_BYTES,
                0,
            )
            .with_isolation(FetchIsolation::ReadUncommitted),
            fetch_decode_limits: FetchDecodeLimits::default(),
            fetch_attempt_timeout: FIRST_GROUP_FETCH_ATTEMPT_TIMEOUT,
            partition_capacity: FIRST_GROUP_FETCH_PARTITIONS,
            effect_capacity: FIRST_GROUP_FETCH_EFFECTS,
            hard_fetch_output_bytes: FIRST_GROUP_FETCH_OUTPUT_BYTES,
            fault: None,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "the internal lossless boundary returns the exact completed position without hidden boxing"
    )]
    pub(in crate::consumer::group) fn try_activate(
        &mut self,
        completed: ClassicGroupPositionCompleted,
        current_fence: GroupPositionFence,
    ) -> Result<(), ClassicGroupFetchActivationError> {
        if self.activation.is_some() || self.fault.is_some() {
            return Err(ClassicGroupFetchActivationError::Returned(
                ClassicGroupFetchActivationFailure::already_active(completed),
            ));
        }
        let input = match prepare_classic_group_fetch_activation(&completed, current_fence) {
            Ok(input) => input,
            Err(error) => {
                return Err(ClassicGroupFetchActivationError::Returned(
                    ClassicGroupFetchActivationFailure::position(completed, error),
                ));
            }
        };
        let partition_count = input.partitions().len();
        if let Err(error) = self.preflight_activation_capacity(partition_count) {
            return Err(ClassicGroupFetchActivationError::Returned(
                ClassicGroupFetchActivationFailure::preflight(completed, input, error),
            ));
        }
        let event_claims = match self.events.prepare_replacement(partition_count) {
            Ok(claims) => claims,
            Err(error) => {
                return Err(ClassicGroupFetchActivationError::Returned(
                    ClassicGroupFetchActivationFailure::preflight(
                        completed,
                        input,
                        ClassicGroupFetchPreflightError::Event(error),
                    ),
                ));
            }
        };
        let transition = match self.machine.install_resolved_assignment(input) {
            Ok(transition) => transition,
            Err(error) => {
                event_claims.rollback_event_claims();
                return Err(ClassicGroupFetchActivationError::Returned(
                    ClassicGroupFetchActivationFailure::core(completed, error),
                ));
            }
        };
        let Some(assignment_epoch) = transition.assignment_epoch() else {
            let kind = ClassicGroupFetchPostCoreFaultKind::MissingAssignmentEpoch;
            event_claims.rollback_event_claims();
            self.fault = Some(ClassicGroupFetchOwnerFault::Activation(
                ClassicGroupFetchActivationFault::new(completed, transition, kind),
            ));
            return Err(ClassicGroupFetchActivationError::Retained(kind));
        };
        let effect_count = transition.effects().len();
        let effect_limit = self.effect_capacity.saturating_sub(self.effects.len());
        if effect_count > effect_limit {
            let kind = ClassicGroupFetchPostCoreFaultKind::EffectCapacity {
                actual: effect_count,
                limit: effect_limit,
            };
            event_claims.rollback_event_claims();
            self.fault = Some(ClassicGroupFetchOwnerFault::Activation(
                ClassicGroupFetchActivationFault::new(completed, transition, kind),
            ));
            return Err(ClassicGroupFetchActivationError::Retained(kind));
        }
        if let Err(error) = event_claims.commit_event_claims(transition.effects()) {
            let kind = ClassicGroupFetchPostCoreFaultKind::Event(error);
            self.fault = Some(ClassicGroupFetchOwnerFault::Activation(
                ClassicGroupFetchActivationFault::new(completed, transition, kind),
            ));
            return Err(ClassicGroupFetchActivationError::Retained(kind));
        }
        let binding = ClassicGroupFetchBinding::new(completed.fence(), assignment_epoch);
        for effect in transition.into_effects() {
            self.effects.push_back(effect);
        }
        self.activation = Some(ClassicGroupFetchActivation::new(binding));
        Ok(())
    }

    pub(in crate::consumer::group) const fn activation(
        &self,
    ) -> Option<&ClassicGroupFetchActivation> {
        self.activation.as_ref()
    }

    pub(in crate::consumer::group) const fn fault(&self) -> Option<&ClassicGroupFetchOwnerFault> {
        self.fault.as_ref()
    }

    fn preflight_activation_capacity(
        &self,
        partition_count: usize,
    ) -> Result<(), ClassicGroupFetchPreflightError> {
        let effects = self.effects.len().saturating_add(partition_count);
        if effects > self.effect_capacity {
            return Err(ClassicGroupFetchPreflightError::EffectCapacity {
                actual: effects,
                limit: self.effect_capacity,
            });
        }
        let prepared = self.pending_fetches.len().saturating_add(partition_count);
        if prepared > self.partition_capacity {
            return Err(ClassicGroupFetchPreflightError::PreparedCapacity {
                actual: prepared,
                limit: self.partition_capacity,
            });
        }
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::consumer::group) const fn machine_assignment_epoch(
        &self,
    ) -> Option<kafka_client_core::AssignmentEpoch> {
        self.machine.assignment_epoch()
    }

    #[cfg(test)]
    pub(super) fn pop_prepared_for_test(&mut self) -> Option<PreparedFetchExecution> {
        self.pending_fetches.pop_front()
    }
}
