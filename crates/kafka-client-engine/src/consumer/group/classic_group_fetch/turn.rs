//! One bounded, stage-ordered execution turn for classic-group Fetch.

use kafka_client_core::{AssignedConsumerInput, AssignedConsumerTransition, Moment};

use crate::{
    clock::MonotonicClock,
    consumer::fetch_execution::{FetchExecutionError, FetchSubmission},
    driver::DriverOwner,
};

use super::{
    super::session_catalog::GroupSessionCatalog,
    model::{
        ClassicGroupFetchFront, ClassicGroupFetchOwnerFault, ClassicGroupFetchTransitionFailure,
    },
    owner::ClassicGroupFetchOwner,
    turn_model::ClassicGroupFetchTurn,
};

impl ClassicGroupFetchOwner {
    /// Interprets, settles, and admits at most one item at each ordered stage.
    pub(in crate::consumer::group) fn turn(
        &mut self,
        catalog: &GroupSessionCatalog,
        clock: &MonotonicClock,
        driver: &DriverOwner,
    ) -> ClassicGroupFetchTurn {
        let mut work = ClassicGroupFetchTurn::default();
        if self.is_faulted() {
            return work;
        }
        let blocked_front = match self.interpret_front_effect(catalog, clock) {
            ClassicGroupFetchFront::Interpreted => {
                work.effect_interpreted = true;
                return work;
            }
            ClassicGroupFetchFront::ControlPending => {
                self.settle_one_fetch(clock, &mut work);
                work.blocked = !work.progressed() && !work.fault_retained;
                return work;
            }
            ClassicGroupFetchFront::Backpressured => true,
            ClassicGroupFetchFront::Idle => false,
        };
        if self.is_faulted() {
            work.fault_retained = true;
            return work;
        }

        if !blocked_front && !self.apply_one_due_timer(clock, &mut work) {
            return work;
        }
        if self.is_faulted() || (!blocked_front && !self.effects.is_empty()) {
            return work;
        }

        let effects_before_poll = self.effects.len();
        self.settle_one_fetch(clock, &mut work);
        if self.is_faulted() || self.effects.len() != effects_before_poll {
            return work;
        }

        self.submit_one_fetch(clock, driver, &mut work);
        if blocked_front && !work.progressed() && !work.fault_retained {
            work.blocked = true;
        }
        work
    }

    fn apply_one_due_timer(
        &mut self,
        clock: &MonotonicClock,
        work: &mut ClassicGroupFetchTurn,
    ) -> bool {
        let Some(now) = self.capture_turn_now(clock, work) else {
            return false;
        };
        let Some(input) = self.timers.pop_due(now) else {
            return true;
        };
        let (applied, retained) = match duplicate_due_input(input) {
            Ok(pair) => pair,
            Err(input) => {
                self.fault =
                    Some(ClassicGroupFetchOwnerFault::UnexpectedTimerInput { _input: input });
                work.fault_retained = true;
                return false;
            }
        };
        match self.machine.apply(applied) {
            Ok(transition) => {
                work.timer_input_applied = true;
                self.append_transition(transition, work)
            }
            Err(error) => {
                self.fault = Some(ClassicGroupFetchOwnerFault::Core {
                    _input: retained,
                    error,
                });
                work.fault_retained = true;
                false
            }
        }
    }

    fn settle_one_fetch(&mut self, clock: &MonotonicClock, work: &mut ClassicGroupFetchTurn) {
        let Some(now) = self.capture_turn_now(clock, work) else {
            return;
        };
        let retained = self.fetches.retained();
        match self.fetches.poll(&mut self.machine, now) {
            Ok(Some(transition)) => {
                work.fetch_polled = true;
                self.append_transition(transition, work);
            }
            Ok(None) => {
                work.fetch_polled = self.fetches.retained() < retained;
            }
            Err(error) => {
                self.fault = Some(ClassicGroupFetchOwnerFault::Fetch(error));
                work.fault_retained = true;
            }
        }
    }

    fn submit_one_fetch(
        &mut self,
        clock: &MonotonicClock,
        driver: &DriverOwner,
        work: &mut ClassicGroupFetchTurn,
    ) {
        let Some(now) = self.capture_turn_now(clock, work) else {
            return;
        };
        let Some(prepared) = self.pending_fetches.pop_front() else {
            return;
        };
        match self
            .fetches
            .submit(driver, &mut self.machine, prepared, now)
        {
            Ok(FetchSubmission::Accepted) => {
                work.fetch_submitted = true;
                work.blocked = false;
            }
            Ok(FetchSubmission::Backpressured(prepared)) => {
                self.pending_fetches.push_front(prepared);
                work.blocked = true;
            }
            Ok(FetchSubmission::Unavailable(prepared)) => {
                self.pending_fetches.push_front(prepared);
                self.fault = Some(ClassicGroupFetchOwnerFault::Fetch(
                    FetchExecutionError::Faulted,
                ));
                work.fault_retained = true;
            }
            Ok(FetchSubmission::Settled(transition)) => {
                work.fetch_submitted = true;
                work.blocked = false;
                if let Some(transition) = transition {
                    self.append_transition(transition, work);
                }
            }
            Err(error) => {
                self.fault = Some(ClassicGroupFetchOwnerFault::Fetch(error));
                work.fault_retained = true;
            }
        }
    }

    fn capture_turn_now(
        &mut self,
        clock: &MonotonicClock,
        work: &mut ClassicGroupFetchTurn,
    ) -> Option<Moment> {
        match clock.now() {
            Ok(now) => Some(now),
            Err(error) => {
                self.fault = Some(ClassicGroupFetchOwnerFault::Clock(error));
                work.fault_retained = true;
                None
            }
        }
    }

    fn append_transition(
        &mut self,
        transition: AssignedConsumerTransition,
        work: &mut ClassicGroupFetchTurn,
    ) -> bool {
        let effect_count = transition.effects().len();
        let actual = self.effects.len().checked_add(effect_count);
        let limit = self.effect_capacity.min(self.effects.capacity());
        if actual.is_none_or(|actual| actual > limit) {
            self.fault = Some(ClassicGroupFetchOwnerFault::Transition {
                _transition: transition,
                failure: ClassicGroupFetchTransitionFailure::EffectCapacity {
                    actual: actual.unwrap_or(usize::MAX),
                    limit,
                },
            });
            work.fault_retained = true;
            return false;
        }
        for effect in transition.into_effects() {
            self.effects.push_back(effect);
        }
        true
    }
}

fn duplicate_due_input(
    input: AssignedConsumerInput,
) -> Result<(AssignedConsumerInput, AssignedConsumerInput), AssignedConsumerInput> {
    match input {
        AssignedConsumerInput::FetchThrottleElapsed { fence, now } => Ok((
            AssignedConsumerInput::FetchThrottleElapsed { fence, now },
            AssignedConsumerInput::FetchThrottleElapsed { fence, now },
        )),
        input => Err(input),
    }
}
