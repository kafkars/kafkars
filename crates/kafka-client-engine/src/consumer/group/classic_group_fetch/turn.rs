//! One bounded, stage-ordered execution turn for classic-group Fetch.

use kafka_client_core::{AssignedConsumerTransition, Moment};

use crate::{
    clock::MonotonicClock,
    consumer::fetch_execution::{FetchExecutionError, FetchSubmission, FetchTerminalPoll},
    driver::DriverOwner,
};

use super::{
    super::session_catalog::GroupSessionCatalog,
    model::{
        ClassicGroupFetchFront, ClassicGroupFetchOwnerFault, ClassicGroupFetchTransitionFailure,
    },
    owner::ClassicGroupFetchOwner,
    position_execution::ClassicGroupPositionStage,
    timer_input::duplicate_due_input,
    turn_model::ClassicGroupFetchTurn,
};

impl ClassicGroupFetchOwner {
    fn poll_terminal_proposal(
        &mut self,
        clock: &MonotonicClock,
        driver: &DriverOwner,
        now: Moment,
    ) -> Result<(Option<AssignedConsumerTransition>, bool), FetchExecutionError> {
        match self.fetches.poll_proposal(now)? {
            FetchTerminalPoll::Proposed(proposal) => self
                .settle_terminal_proposal(clock, driver, now, proposal)
                .map(|transition| (transition, true)),
            FetchTerminalPoll::Progressed => Ok((None, true)),
            FetchTerminalPoll::Idle => Ok((None, false)),
        }
    }

    /// Interprets, settles, and admits at most one item at each ordered stage.
    pub(in crate::consumer::group) fn turn(
        &mut self,
        catalog: &GroupSessionCatalog,
        clock: &MonotonicClock,
        driver: &DriverOwner,
    ) -> ClassicGroupFetchTurn {
        let mut work = ClassicGroupFetchTurn::default();
        if self.is_faulted() {
            self.settle_seek_host_unavailable();
            return work;
        }
        let blocked_front = match self.interpret_front_effect(catalog, clock) {
            ClassicGroupFetchFront::Interpreted => {
                work.effect_interpreted = true;
                return work;
            }
            ClassicGroupFetchFront::ControlPending => {
                self.settle_one_fetch(clock, driver, &mut work);
                work.blocked = !work.progressed() && !work.fault_retained;
                return work;
            }
            ClassicGroupFetchFront::Backpressured => true,
            ClassicGroupFetchFront::Idle => false,
        };
        if self.is_faulted() {
            self.settle_seek_host_unavailable();
            work.fault_retained = true;
            return work;
        }
        if !blocked_front && !self.apply_one_due_timer(clock, &mut work) {
            return work;
        }
        if self.is_faulted() || (!blocked_front && !self.effects.is_empty()) {
            return work;
        }

        work.position_polled = matches!(
            self.poll_seek_position(clock),
            ClassicGroupPositionStage::Progressed
        );
        if self.is_faulted() || !self.effects.is_empty() {
            return work;
        }

        if !self.fetches.broker_session_close_requested()
            && !self.pending_fetches.is_empty()
            && self.fetches.broker_sessions_have_forgotten_ready()
        {
            self.submit_one_fetch(clock, driver, &mut work);
            if work.fetch_submitted || work.fault_retained {
                return work;
            }
        }

        let effects_before_poll = self.effects.len();
        self.settle_one_fetch(clock, driver, &mut work);
        if self.is_faulted() || self.effects.len() != effects_before_poll {
            return work;
        }

        work.position_submitted = matches!(
            self.submit_seek_position(clock, driver),
            ClassicGroupPositionStage::Progressed
        );
        if self.is_faulted() || !self.effects.is_empty() {
            return work;
        }

        if self.fetches.broker_session_close_requested() {
            self.close_retired_broker_sessions(clock, driver, &mut work);
            work.blocked = !work.progressed() && !work.fault_retained;
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
                self.settle_seek_host_unavailable();
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
                self.settle_seek_host_unavailable();
                work.fault_retained = true;
                false
            }
        }
    }

    fn settle_one_fetch(
        &mut self,
        clock: &MonotonicClock,
        driver: &DriverOwner,
        work: &mut ClassicGroupFetchTurn,
    ) {
        let Some(now) = self.capture_turn_now(clock, work) else {
            return;
        };
        let retained = self.fetches.retained();
        match self
            .fetches
            .drive_broker_fetches(driver, &mut self.machine, clock, now)
        {
            Ok((Some(transition), _progressed)) => {
                work.fetch_polled = true;
                self.append_transition(transition, work);
                return;
            }
            Ok((None, true)) => {
                work.fetch_polled = true;
                return;
            }
            Ok((None, false)) => {}
            Err(error) => {
                self.fault = Some(ClassicGroupFetchOwnerFault::Fetch(error));
                self.settle_seek_host_unavailable();
                work.fault_retained = true;
                return;
            }
        }
        match self.poll_terminal_proposal(clock, driver, now) {
            Ok((Some(transition), progressed)) => {
                work.fetch_polled = progressed;
                self.append_transition(transition, work);
            }
            Ok((None, progressed)) => {
                work.fetch_polled = progressed || self.fetches.retained() < retained;
                work.fault_retained = self.is_faulted();
            }
            Err(error) => {
                self.fault = Some(ClassicGroupFetchOwnerFault::Fetch(error));
                self.settle_seek_host_unavailable();
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
                self.settle_seek_host_unavailable();
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
                self.settle_seek_host_unavailable();
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
                self.settle_seek_host_unavailable();
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
            self.settle_seek_host_unavailable();
            work.fault_retained = true;
            return false;
        }
        for effect in transition.into_effects() {
            self.effects.push_back(effect);
        }
        true
    }
}
