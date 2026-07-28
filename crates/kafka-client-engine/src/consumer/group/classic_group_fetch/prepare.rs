//! Single-front FIFO interpretation at the group Fetch attempt boundary.

use kafka_client_core::{AssignedConsumerEffect, Deadline};

use crate::{
    clock::MonotonicClock,
    consumer::fetch_execution::{
        FetchAttemptDeadline, FetchExecutionError, PrepareFetchFailure, PreparedFetchExecution,
    },
};

use super::{
    super::session_catalog::GroupSessionCatalog,
    model::{
        ClassicGroupFetchCapturedFailure, ClassicGroupFetchEffectFailure, ClassicGroupFetchFront,
        ClassicGroupFetchOwnerFault, minimum_deadline,
    },
    owner::ClassicGroupFetchOwner,
};

impl ClassicGroupFetchOwner {
    pub(in crate::consumer::group) fn interpret_front_effect(
        &mut self,
        catalog: &GroupSessionCatalog,
        clock: &MonotonicClock,
    ) -> ClassicGroupFetchFront {
        if self.fault.is_some() {
            return ClassicGroupFetchFront::Idle;
        }
        let Some(effect) = self.effects.front().copied() else {
            return ClassicGroupFetchFront::Idle;
        };
        if is_control(effect) {
            return self.interpret_control(effect);
        }
        let result = match effect {
            AssignedConsumerEffect::ArmFetchThrottle { fence, deadline } => self
                .timers
                .arm_fetch(fence, deadline)
                .map(|_disposition| ())
                .map_err(ClassicGroupFetchEffectFailure::Timer),
            AssignedConsumerEffect::FetchReady { fence, .. } => {
                if self.pending_fetches.len() >= self.partition_capacity {
                    return ClassicGroupFetchFront::Backpressured;
                }
                match self.prepare_fetch(effect, fence, catalog, clock) {
                    Ok(()) => Ok(()),
                    Err(ClassicGroupFetchPrepareFailure::Effect(failure)) => Err(failure),
                    Err(ClassicGroupFetchPrepareFailure::Captured { attempt, failure }) => {
                        self.fault = Some(ClassicGroupFetchOwnerFault::Captured {
                            effect,
                            attempt,
                            failure,
                        });
                        return ClassicGroupFetchFront::Idle;
                    }
                }
            }
            AssignedConsumerEffect::AuthorizeFetchDelivery { .. } => Ok(()),
            AssignedConsumerEffect::AcceptClose { .. }
            | AssignedConsumerEffect::CompleteClose { .. }
            | AssignedConsumerEffect::ResolvePosition { .. }
            | AssignedConsumerEffect::PositionResolutionFailed { .. }
            | AssignedConsumerEffect::ArmPositionThrottle { .. }
            | AssignedConsumerEffect::FetchThrottleFailed { .. }
            | AssignedConsumerEffect::FetchFailed { .. }
            | AssignedConsumerEffect::Revoke { .. }
            | AssignedConsumerEffect::Suspend { .. } => {
                return ClassicGroupFetchFront::Idle;
            }
        };
        match result {
            Ok(()) => {
                if let Err(error) = self.events.observe_effect(effect) {
                    self.fault = Some(ClassicGroupFetchOwnerFault::Effect {
                        effect,
                        failure: ClassicGroupFetchEffectFailure::Event(error),
                    });
                    return ClassicGroupFetchFront::Idle;
                }
                self.effects.pop_front();
                ClassicGroupFetchFront::Interpreted
            }
            Err(failure) => {
                self.fault = Some(ClassicGroupFetchOwnerFault::Effect { effect, failure });
                ClassicGroupFetchFront::Idle
            }
        }
    }

    pub(in crate::consumer::group) fn next_deadline(&self) -> Option<Deadline> {
        if self.fault.is_some() {
            return None;
        }
        let mut next = self.timers.next_deadline();
        for pending in &self.pending_fetches {
            next = minimum_deadline(next, pending.deadline());
        }
        next
    }

    #[allow(
        clippy::result_large_err,
        reason = "the failure retains the exact captured attempt deadline"
    )]
    fn prepare_fetch(
        &mut self,
        effect: AssignedConsumerEffect,
        fence: kafka_client_core::FetchFence,
        catalog: &GroupSessionCatalog,
        clock: &MonotonicClock,
    ) -> Result<(), ClassicGroupFetchPrepareFailure> {
        let attempt =
            FetchAttemptDeadline::capture_for_fetch(fence, clock, self.fetch_attempt_timeout)
                .map_err(|error| {
                    ClassicGroupFetchPrepareFailure::Effect(ClassicGroupFetchEffectFailure::Clock(
                        error,
                    ))
                })?;
        let topic = match catalog.copy_topic_name(fence.position().partition().topic_id()) {
            Ok(topic) => topic,
            Err(error) => {
                return Err(ClassicGroupFetchPrepareFailure::Captured {
                    attempt,
                    failure: ClassicGroupFetchCapturedFailure::Catalog(error),
                });
            }
        };
        let prepared = match PreparedFetchExecution::new_retaining_attempt(
            effect,
            topic,
            self.fetch_settings,
            self.fetch_decode_limits,
            attempt,
            self.hard_fetch_output_bytes,
        ) {
            Ok(prepared) => prepared,
            Err(failure) => {
                let failure: PrepareFetchFailure = failure;
                let (error, attempt) = failure.into_parts();
                return Err(ClassicGroupFetchPrepareFailure::Captured {
                    attempt,
                    failure: ClassicGroupFetchCapturedFailure::Preparation(error),
                });
            }
        };
        self.pending_fetches.push_back(prepared);
        Ok(())
    }

    fn interpret_control(&mut self, effect: AssignedConsumerEffect) -> ClassicGroupFetchFront {
        match self.fetches.observe_control(effect) {
            Ok(()) => {}
            Err(FetchExecutionError::ControlPending(_pending)) => {
                return ClassicGroupFetchFront::ControlPending;
            }
            Err(error) => {
                self.fault = Some(ClassicGroupFetchOwnerFault::Fetch(error));
                return ClassicGroupFetchFront::Idle;
            }
        }
        self.timers.observe_control(effect);
        if !self.reconcile_pending_fetches() {
            return ClassicGroupFetchFront::Idle;
        }
        if let Err(error) = self.events.observe_effect(effect) {
            self.fault = Some(ClassicGroupFetchOwnerFault::Effect {
                effect,
                failure: ClassicGroupFetchEffectFailure::Event(error),
            });
            return ClassicGroupFetchFront::Idle;
        }
        self.effects.pop_front();
        ClassicGroupFetchFront::Interpreted
    }

    fn reconcile_pending_fetches(&mut self) -> bool {
        let retained = self.pending_fetches.len();
        for _index in 0..retained {
            let Some(prepared) = self.pending_fetches.pop_front() else {
                return true;
            };
            match prepared.reconcile_ownership(&self.machine) {
                Ok(Some(prepared)) => self.pending_fetches.push_back(prepared),
                Ok(None) => {}
                Err((kafka_client_core::AssignedConsumerMachineError::NoAssignment, _prepared)) => {
                    // Assignment retirement precedes reconciliation of its
                    // queued local Fetch preparations. Those preparations are
                    // now directionally superseded by the close fence.
                }
                Err((error, prepared)) => {
                    self.fault = Some(ClassicGroupFetchOwnerFault::Pending {
                        error,
                        _prepared: prepared,
                    });
                    return false;
                }
            }
        }
        true
    }

    #[cfg(test)]
    pub(super) fn front_effect_for_test(&self) -> Option<AssignedConsumerEffect> {
        self.effects.front().copied()
    }

    #[cfg(test)]
    pub(super) fn effect_count_for_test(&self) -> usize {
        self.effects.len()
    }

    #[cfg(test)]
    pub(super) fn pending_count_for_test(&self) -> usize {
        self.pending_fetches.len()
    }

    #[cfg(test)]
    pub(super) const fn timer_count_for_test(&self) -> usize {
        self.timers.timer_count()
    }
}

#[must_use = "Fetch preparation failure retains the captured attempt when one exists"]
enum ClassicGroupFetchPrepareFailure {
    Effect(ClassicGroupFetchEffectFailure),
    Captured {
        attempt: FetchAttemptDeadline,
        failure: ClassicGroupFetchCapturedFailure,
    },
}

const fn is_control(effect: AssignedConsumerEffect) -> bool {
    matches!(
        effect,
        AssignedConsumerEffect::Revoke { .. } | AssignedConsumerEffect::Suspend { .. }
    )
}
