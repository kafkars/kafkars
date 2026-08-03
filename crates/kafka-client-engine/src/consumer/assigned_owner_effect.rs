//! Single-front FIFO interpretation with exact control and deadline ordering.

use kafka_client_core::AssignedConsumerEffect;

use super::{
    assigned_owner::AssignedConsumerOwner,
    assigned_owner_event::is_terminal_event,
    assigned_owner_fault::{AssignedConsumerEffectFailure, AssignedConsumerOwnerFault},
    assigned_owner_model::{PendingPosition, fetch_isolation, position_isolation},
    fetch_execution::{FetchAttemptDeadline, FetchExecutionError, PreparedFetchExecution},
    position_execution::PreparedPositionResolution,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FrontEffect {
    Idle,
    Interpreted,
    ControlPending,
}

impl AssignedConsumerOwner {
    pub(super) fn interpret_front_effect(&mut self) -> FrontEffect {
        if self.is_faulted() {
            return FrontEffect::Idle;
        }
        let Some(effect) = self.effects.front().copied() else {
            return FrontEffect::Idle;
        };
        if is_terminal_event(effect) {
            return self.retain_terminal_event(effect);
        }
        if is_control(effect) {
            return self.interpret_control(effect);
        }
        let result = match effect {
            AssignedConsumerEffect::AcceptClose { .. } => self
                .close
                .observe_close_effect(effect)
                .map(|()| self.fetches.request_broker_session_close())
                .map_err(AssignedConsumerEffectFailure::Close),
            AssignedConsumerEffect::CompleteClose { .. } => self
                .close
                .observe_close_effect(effect)
                .map_err(AssignedConsumerEffectFailure::Close),
            AssignedConsumerEffect::ResolvePosition { fence, .. } => {
                self.prepare_position(effect, fence)
            }
            AssignedConsumerEffect::ArmPositionThrottle { fence, deadline } => self
                .timers
                .arm_position(fence, deadline)
                .map(|_disposition| ())
                .map_err(AssignedConsumerEffectFailure::Timer),
            AssignedConsumerEffect::ArmFetchThrottle { fence, deadline } => self
                .timers
                .arm_fetch(fence, deadline)
                .map(|_disposition| ())
                .map_err(AssignedConsumerEffectFailure::Timer),
            AssignedConsumerEffect::FetchReady { fence, .. } => self.prepare_fetch(effect, fence),
            AssignedConsumerEffect::AuthorizeFetchDelivery { .. } => Ok(()),
            AssignedConsumerEffect::PositionResolutionFailed { .. }
            | AssignedConsumerEffect::FetchThrottleFailed { .. }
            | AssignedConsumerEffect::FetchFailed { .. } => return FrontEffect::Idle,
            AssignedConsumerEffect::Revoke { .. } | AssignedConsumerEffect::Suspend { .. } => {
                return FrontEffect::Idle;
            }
        };
        match result {
            Ok(()) => {
                if let Err(failure) = self.events.observe_effect(effect) {
                    self.fault = Some(AssignedConsumerOwnerFault::Effect {
                        effect,
                        failure: AssignedConsumerEffectFailure::Event(failure),
                    });
                    return FrontEffect::Idle;
                }
                self.effects.pop_front();
                FrontEffect::Interpreted
            }
            Err(failure) => {
                self.fault = Some(AssignedConsumerOwnerFault::Effect { effect, failure });
                FrontEffect::Idle
            }
        }
    }

    fn interpret_control(&mut self, effect: AssignedConsumerEffect) -> FrontEffect {
        match self.fetches.observe_control(effect) {
            Ok(()) => {}
            Err(FetchExecutionError::ControlPending(_pending)) => {
                return FrontEffect::ControlPending;
            }
            Err(error) => {
                self.fault = Some(AssignedConsumerOwnerFault::Fetch(error));
                return FrontEffect::Idle;
            }
        }
        self.positions.observe_control(effect);
        self.timers.observe_control(effect);
        if !self.reconcile_pending_positions() || !self.reconcile_pending_fetches() {
            return FrontEffect::Idle;
        }
        if let Err(failure) = self.events.observe_effect(effect) {
            self.fault = Some(AssignedConsumerOwnerFault::Effect {
                effect,
                failure: AssignedConsumerEffectFailure::Event(failure),
            });
            return FrontEffect::Idle;
        }
        self.effects.pop_front();
        FrontEffect::Interpreted
    }

    fn prepare_position(
        &mut self,
        effect: AssignedConsumerEffect,
        fence: kafka_client_core::PositionFence,
    ) -> Result<(), AssignedConsumerEffectFailure> {
        if self.pending_positions.len() >= self.limits.partition_capacity {
            return Err(AssignedConsumerEffectFailure::PendingCapacity);
        }
        let Some(retained) = self.raw_position_deadlines.front().copied() else {
            return Err(AssignedConsumerEffectFailure::PositionDeadlineMissing);
        };
        if retained.fence != fence {
            return Err(AssignedConsumerEffectFailure::PositionDeadlineMismatch {
                expected: fence,
                supplied: retained.fence,
            });
        }
        let topic = self
            .topics
            .copy_name(fence.partition().topic_id())
            .map_err(AssignedConsumerEffectFailure::from)?;
        let prepared = PreparedPositionResolution::new(
            effect,
            topic,
            position_isolation(self.read_isolation()),
            retained.deadline,
        )
        .map_err(AssignedConsumerEffectFailure::PositionPreparation)?;
        self.raw_position_deadlines.pop_front();
        self.pending_positions.push_back(PendingPosition {
            prepared,
            deadline: retained.deadline,
        });
        Ok(())
    }

    fn prepare_fetch(
        &mut self,
        effect: AssignedConsumerEffect,
        fence: kafka_client_core::FetchFence,
    ) -> Result<(), AssignedConsumerEffectFailure> {
        if self.pending_fetches.len() >= self.limits.partition_capacity {
            return Err(AssignedConsumerEffectFailure::PendingCapacity);
        }
        let attempt = FetchAttemptDeadline::capture_for_fetch(
            fence,
            &self.clock,
            self.settings.fetch_attempt_timeout,
        )
        .map_err(AssignedConsumerEffectFailure::Clock)?;
        let topic = self
            .topics
            .copy_name(fence.position().partition().topic_id())
            .map_err(AssignedConsumerEffectFailure::from)?;
        let prepared = PreparedFetchExecution::new(
            effect,
            topic,
            self.settings
                .fetch_settings
                .with_isolation(fetch_isolation(self.read_isolation())),
            self.settings.fetch_decode_limits,
            attempt,
            self.limits.hard_fetch_output_bytes,
        )
        .map_err(AssignedConsumerEffectFailure::FetchPreparation)?;
        self.pending_fetches.push_back(prepared);
        Ok(())
    }

    fn reconcile_pending_positions(&mut self) -> bool {
        let retained = self.pending_positions.len();
        for _index in 0..retained {
            let Some(pending) = self.pending_positions.pop_front() else {
                return true;
            };
            match pending.prepared.reconcile_ownership(&self.machine) {
                Ok(Some(prepared)) => self.pending_positions.push_back(PendingPosition {
                    prepared,
                    deadline: pending.deadline,
                }),
                Ok(None) => {}
                Err((error, prepared)) => {
                    self.fault = Some(AssignedConsumerOwnerFault::PendingPosition {
                        error,
                        pending: PendingPosition {
                            prepared,
                            deadline: pending.deadline,
                        },
                    });
                    return false;
                }
            }
        }
        true
    }

    fn reconcile_pending_fetches(&mut self) -> bool {
        let retained = self.pending_fetches.len();
        for _index in 0..retained {
            let Some(pending) = self.pending_fetches.pop_front() else {
                return true;
            };
            match pending.reconcile_ownership(&self.machine) {
                Ok(Some(pending)) => self.pending_fetches.push_back(pending),
                Ok(None) => {}
                Err((error, pending)) => {
                    self.fault = Some(AssignedConsumerOwnerFault::PendingFetch { error, pending });
                    return false;
                }
            }
        }
        true
    }
}

const fn is_control(effect: AssignedConsumerEffect) -> bool {
    matches!(
        effect,
        AssignedConsumerEffect::Revoke { .. } | AssignedConsumerEffect::Suspend { .. }
    )
}
