//! Bounded submission and settlement of group seek `ListOffsets` work.

use kafka_client_core::{AssignedConsumerInput, Moment};

use crate::{
    clock::MonotonicClock,
    consumer::{assigned_owner_model::PendingPosition, position_execution::PositionSubmission},
    driver::DriverOwner,
};

use super::{
    model::{ClassicGroupFetchOwnerFault, ClassicGroupFetchTransitionFailure},
    owner::ClassicGroupFetchOwner,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicGroupPositionStage {
    Idle,
    Progressed,
    Backpressured,
    Faulted,
}

impl ClassicGroupFetchOwner {
    pub(super) fn poll_seek_position(
        &mut self,
        clock: &MonotonicClock,
    ) -> ClassicGroupPositionStage {
        let Some(now) = self.position_now(clock) else {
            return ClassicGroupPositionStage::Faulted;
        };
        let retained = self.positions.retained_positions();
        match self.positions.poll(&mut self.machine, now) {
            Ok(Some(transition)) => {
                self.append_position_transition(transition);
                ClassicGroupPositionStage::Progressed
            }
            Ok(None) if self.positions.retained_positions() < retained => {
                ClassicGroupPositionStage::Progressed
            }
            Ok(None) => ClassicGroupPositionStage::Idle,
            Err(error) => {
                self.fault = Some(ClassicGroupFetchOwnerFault::Position(error));
                self.settle_seek_host_unavailable();
                ClassicGroupPositionStage::Faulted
            }
        }
    }

    pub(super) fn submit_seek_position(
        &mut self,
        clock: &MonotonicClock,
        driver: &DriverOwner,
    ) -> ClassicGroupPositionStage {
        let Some(now) = self.position_now(clock) else {
            return ClassicGroupPositionStage::Faulted;
        };
        let Some(pending) = self.pending_positions.pop_front() else {
            return ClassicGroupPositionStage::Idle;
        };
        if pending.deadline.core().is_elapsed_at(now) {
            return self.expire_pending_position(pending, now);
        }
        let PendingPosition { prepared, deadline } = pending;
        match self
            .positions
            .submit(driver, &mut self.machine, prepared, now)
        {
            Ok(PositionSubmission::Accepted) => ClassicGroupPositionStage::Progressed,
            Ok(PositionSubmission::Backpressured(prepared)) => {
                self.pending_positions
                    .push_front(PendingPosition { prepared, deadline });
                ClassicGroupPositionStage::Backpressured
            }
            Ok(PositionSubmission::Settled(transition)) => {
                if let Some(transition) = transition {
                    self.append_position_transition(transition);
                }
                ClassicGroupPositionStage::Progressed
            }
            Err(error) => {
                self.fault = Some(ClassicGroupFetchOwnerFault::Position(error));
                self.settle_seek_host_unavailable();
                ClassicGroupPositionStage::Faulted
            }
        }
    }

    fn expire_pending_position(
        &mut self,
        pending: PendingPosition,
        now: Moment,
    ) -> ClassicGroupPositionStage {
        let fence = pending.prepared.fence();
        let input = AssignedConsumerInput::PositionResolutionDeadlineElapsed { fence, now };
        match self.machine.apply(input) {
            Ok(transition) => {
                self.append_position_transition(transition);
                ClassicGroupPositionStage::Progressed
            }
            Err(error) => {
                self.fault = Some(ClassicGroupFetchOwnerFault::PendingPosition {
                    error,
                    _pending: pending,
                });
                self.settle_seek_host_unavailable();
                ClassicGroupPositionStage::Faulted
            }
        }
    }

    fn append_position_transition(
        &mut self,
        transition: kafka_client_core::AssignedConsumerTransition,
    ) {
        let actual = self.effects.len().checked_add(transition.effects().len());
        if actual.is_none_or(|actual| actual > self.effect_capacity) {
            let failure = ClassicGroupFetchTransitionFailure::EffectCapacity {
                actual: actual.unwrap_or(usize::MAX),
                limit: self.effect_capacity,
            };
            self.fault = Some(ClassicGroupFetchOwnerFault::Transition {
                _transition: transition,
                failure,
            });
            self.settle_seek_host_unavailable();
            return;
        }
        self.settle_seek_transition(&transition);
        self.effects.extend(transition.into_effects());
    }

    fn position_now(&mut self, clock: &MonotonicClock) -> Option<Moment> {
        match clock.now() {
            Ok(now) => Some(now),
            Err(error) => {
                self.fault = Some(ClassicGroupFetchOwnerFault::Clock(error));
                self.settle_seek_host_unavailable();
                None
            }
        }
    }
}
