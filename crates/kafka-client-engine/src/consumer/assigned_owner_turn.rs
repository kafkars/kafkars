//! Deterministic bounded stages for one assigned-consumer owner turn.

use crate::driver::DriverOwner;

use super::{
    assigned_owner::AssignedConsumerOwner, assigned_owner_effect::FrontEffect,
    assigned_owner_fault::AssignedConsumerOwnerFault, assigned_owner_model::AssignedConsumerTurn,
    assigned_owner_pending::PendingAttempt,
};

impl AssignedConsumerOwner {
    /// Performs one bounded turn while borrowing the embedded driver owner only here.
    pub(crate) fn turn(&mut self, driver: &DriverOwner) -> AssignedConsumerTurn {
        let mut work = AssignedConsumerTurn::default();
        if self.is_faulted() {
            return work;
        }
        match self.interpret_front_effect() {
            FrontEffect::Interpreted => {
                work.effect_interpreted = true;
                return work;
            }
            FrontEffect::ControlPending => {
                let Some(now) = self.capture_turn_now() else {
                    return work;
                };
                let retained = self.fetches.retained();
                match self.fetches.poll(&mut self.machine, now) {
                    Ok(Some(transition)) => {
                        work.fetch_polled = true;
                        self.retain_transition(transition, None);
                    }
                    Ok(None) => {
                        work.fetch_polled = self.fetches.retained() < retained;
                    }
                    Err(error) => {
                        work.fetch_polled = true;
                        self.fault = Some(AssignedConsumerOwnerFault::Fetch(error));
                    }
                }
                return work;
            }
            FrontEffect::Idle => {}
        }
        let Some(now) = self.capture_turn_now() else {
            return work;
        };
        if self.is_faulted() || !self.effects.is_empty() {
            return work;
        }
        while work.timer_inputs < self.settings.due_timer_budget {
            let Some(input) = self.timers.pop_due(now) else {
                break;
            };
            let Some(retained_input) = duplicate_timer_input(&input) else {
                self.fault = Some(AssignedConsumerOwnerFault::UnexpectedTimerInput(input));
                return work;
            };
            match self.machine.apply(input) {
                Ok(transition) => {
                    work.timer_inputs += 1;
                    self.enqueue_transition(transition, None);
                    if self.is_faulted() || !self.effects.is_empty() {
                        return work;
                    }
                }
                Err(error) => {
                    self.fault = Some(AssignedConsumerOwnerFault::Core {
                        input: retained_input,
                        error,
                    });
                    return work;
                }
            }
        }
        let Some(now) = self.capture_turn_now() else {
            return work;
        };
        work.position_polled = self.poll_position(now);
        if self.is_faulted() || !self.effects.is_empty() {
            return work;
        }
        let Some(now) = self.capture_turn_now() else {
            return work;
        };
        work.fetch_polled = self.drive_and_poll_fetch_executor(driver, now);
        if self.is_faulted() || !self.effects.is_empty() {
            return work;
        }
        let Some(now) = self.capture_turn_now() else {
            return work;
        };
        work.position_submitted = matches!(
            self.submit_position(driver, now),
            PendingAttempt::Progressed
        );
        if self.is_faulted() || !self.effects.is_empty() {
            return work;
        }
        let Some(now) = self.capture_turn_now() else {
            return work;
        };
        work.fetch_submitted = matches!(self.submit_fetch(driver, now), PendingAttempt::Progressed);
        if self.is_faulted() || !self.effects.is_empty() {
            return work;
        }
        work.close_progressed = self.progress_broker_session_close(driver);
        if self.is_faulted() || work.close_progressed {
            return work;
        }
        work.close_progressed = self.progress_close();
        work
    }

    fn capture_turn_now(&mut self) -> Option<kafka_client_core::Moment> {
        match self.clock.now() {
            Ok(now) => Some(now),
            Err(error) => {
                self.fault = Some(AssignedConsumerOwnerFault::Clock(error));
                None
            }
        }
    }
}

fn duplicate_timer_input(
    input: &kafka_client_core::AssignedConsumerInput,
) -> Option<kafka_client_core::AssignedConsumerInput> {
    match input {
        kafka_client_core::AssignedConsumerInput::PositionThrottleElapsed { fence, now } => Some(
            kafka_client_core::AssignedConsumerInput::PositionThrottleElapsed {
                fence: *fence,
                now: *now,
            },
        ),
        kafka_client_core::AssignedConsumerInput::FetchThrottleElapsed { fence, now } => Some(
            kafka_client_core::AssignedConsumerInput::FetchThrottleElapsed {
                fence: *fence,
                now: *now,
            },
        ),
        _ => None,
    }
}
