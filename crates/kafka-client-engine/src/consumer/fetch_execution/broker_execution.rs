//! Metadata-routed broker Fetch-session admission and exact plan ownership.

use crate::clock::MonotonicClock;
use crate::driver::{BrokerFetchRouteCall, BrokerFetchRouteFailureKind, BrokerId, DriverOwner};
use kafka_client_core::{
    AssignedConsumerMachine, AssignedConsumerTransition, FetchFailure, Moment,
};

use super::{
    broker_batch::broker_session_members,
    broker_session::BrokerSessionPlan,
    executor::DirectFetchExecutor,
    fault::{FetchExecutionError, RetainedFetchFault},
    prepared::PreparedFetchExecution,
};

pub(super) struct PendingBrokerRoute {
    pub(super) call: BrokerFetchRouteCall,
    pub(super) hard_output_bytes: usize,
}

pub(super) struct RoutedBrokerFetch {
    pub(super) broker_id: BrokerId,
    pub(super) request: crate::driver::PartitionFetchRequest,
    pub(super) hard_output_bytes: usize,
}

pub(super) struct ActiveBrokerSession {
    pub(super) fences: Vec<kafka_client_core::FetchFence>,
    pub(super) plan: BrokerSessionPlan,
    pub(super) update: Option<crate::protocol::fetch::FetchSessionUpdate>,
    pub(super) reset: bool,
}
impl DirectFetchExecutor {
    pub(super) fn submit_broker_route(
        &mut self,
        driver: &DriverOwner,
        machine: &mut AssignedConsumerMachine,
        prepared: PreparedFetchExecution,
        now: Moment,
    ) -> Result<super::FetchSubmission, FetchExecutionError> {
        if self.route_calls.len().saturating_add(self.routed.len()) >= self.route_capacity {
            return Ok(super::FetchSubmission::Backpressured(prepared));
        }
        if prepared.deadline().is_elapsed_at(now) {
            return self.settle_unadmitted(machine, prepared, FetchFailure::DeadlineElapsed);
        }
        if let Some((broker_id, topic_id, leader_epoch)) = self
            .broker_sessions
            .as_ref()
            .and_then(|sessions| sessions.route_for_position(prepared.fence().position()))
        {
            let (mut request, hard_output_bytes) = prepared.into_parts();
            request.bind_cached_topic_route(topic_id, leader_epoch);
            self.routed.push(RoutedBrokerFetch {
                broker_id,
                request,
                hard_output_bytes,
            });
            return Ok(super::FetchSubmission::Accepted);
        }
        // One TopicView call reserves its worst-case projection bytes. Keep that
        // ownership linear and retain later Fetches for the next bounded turn.
        if !self.route_calls.is_empty() {
            return Ok(super::FetchSubmission::Backpressured(prepared));
        }
        let (request, hard_output_bytes) = prepared.into_parts();
        match BrokerFetchRouteCall::submit(driver, request) {
            Ok(call) => {
                self.route_calls.push(PendingBrokerRoute {
                    call,
                    hard_output_bytes,
                });
                Ok(super::FetchSubmission::Accepted)
            }
            Err(failure) => {
                let (request, kind) = failure.into_parts();
                let prepared = PreparedFetchExecution::from_parts(request, hard_output_bytes);
                match kind {
                    BrokerFetchRouteFailureKind::Backpressured => {
                        Ok(super::FetchSubmission::Backpressured(prepared))
                    }
                    BrokerFetchRouteFailureKind::Terminal(failure) => {
                        self.settle_unadmitted(machine, prepared, failure)
                    }
                    BrokerFetchRouteFailureKind::Completion => {
                        self.fault = Some(RetainedFetchFault::Prepared {
                            _prepared: prepared,
                        });
                        Err(FetchExecutionError::BrokerRouteCompletion)
                    }
                }
            }
        }
    }

    pub(crate) fn drive_broker_fetches(
        &mut self,
        driver: &DriverOwner,
        machine: &mut AssignedConsumerMachine,
        clock: &MonotonicClock,
        now: Moment,
    ) -> Result<(Option<AssignedConsumerTransition>, bool), FetchExecutionError> {
        if self.broker_sessions.is_none() || self.fault.is_some() {
            return Ok((None, false));
        }
        if self.leader_recovery.poll(driver) {
            return Ok((None, true));
        }
        if let Some(waiting) = self.leader_recovery.take_waiting() {
            return self.drive_waiting_leader_route(driver, machine, waiting, now);
        }
        let routed_before = self.routed.len();
        let route_calls_before = self.route_calls.len();
        if let Some(transition) = self.poll_one_broker_route(machine)? {
            return Ok((Some(transition), true));
        }
        if self.routed.len() > routed_before {
            return Ok((None, true));
        }
        if self.route_calls.len() < route_calls_before {
            return Ok((None, true));
        }
        // Routed partitions accumulate until the admitted projection phase is
        // terminal, then one broker plan consumes every same-broker member.
        if !self.route_calls.is_empty() {
            return Ok((None, false));
        }
        if self.broker_maintenance.is_some() {
            let progressed = self.drive_forgotten_maintenance(driver, clock, now)?;
            return Ok((None, progressed));
        }
        if !self.routed.is_empty() {
            let routed_before = self.routed.len();
            let transition = self.dispatch_one_routed(driver, machine, now)?;
            let progressed = transition.is_some() || self.routed.len() < routed_before;
            return Ok((transition, progressed));
        }
        if self.drive_forgotten_maintenance(driver, clock, now)? {
            return Ok((None, true));
        }
        if self.broker_maintenance.is_some() {
            return Ok((None, false));
        }
        Ok((None, false))
    }

    fn dispatch_one_routed(
        &mut self,
        driver: &DriverOwner,
        machine: &mut AssignedConsumerMachine,
        now: Moment,
    ) -> Result<Option<AssignedConsumerTransition>, FetchExecutionError> {
        let Some(routed) = self.routed.pop() else {
            return Ok(None);
        };
        let prepared = PreparedFetchExecution::from_parts(routed.request, routed.hard_output_bytes);
        let prepared = match prepared.reconcile_ownership(machine) {
            Ok(Some(prepared)) => prepared,
            Ok(None) => return Ok(None),
            Err((error, prepared)) => {
                self.fault = Some(RetainedFetchFault::Prepared {
                    _prepared: prepared,
                });
                return Err(FetchExecutionError::Core(error));
            }
        };
        if prepared.deadline().is_elapsed_at(now) {
            return match self.settle_unadmitted(machine, prepared, FetchFailure::DeadlineElapsed)? {
                super::FetchSubmission::Settled(transition) => Ok(transition),
                _ => unreachable!("deadline settles immediately"),
            };
        }
        let mut prepared_batch = vec![prepared];
        self.collect_same_broker_routed(routed.broker_id, machine, now, &mut prepared_batch);
        let members = broker_session_members(&prepared_batch);
        let sessions = self
            .broker_sessions
            .as_mut()
            .unwrap_or_else(|| unreachable!("broker routing requires sessions"));
        let plan = match sessions.try_begin(routed.broker_id, members) {
            Ok(plan) => plan,
            Err((super::broker_session_state::BrokerSessionError::InFlight, _active)) => {
                for prepared in prepared_batch {
                    self.restore_routed(routed.broker_id, prepared);
                }
                return Ok(None);
            }
            Err((_error, _active)) => {
                let prepared = prepared_batch
                    .pop()
                    .unwrap_or_else(|| unreachable!("nonempty broker Fetch batch"));
                for retained in prepared_batch {
                    self.restore_routed(routed.broker_id, retained);
                }
                return match self.settle_unadmitted(
                    machine,
                    prepared,
                    FetchFailure::DriverRejected,
                )? {
                    super::FetchSubmission::Settled(transition) => Ok(transition),
                    _ => unreachable!("session capacity settles immediately"),
                };
            }
        };
        for prepared in &mut prepared_batch {
            prepared.request.bind_session(plan.session());
        }
        self.submit_broker_plan(driver, machine, routed.broker_id, prepared_batch, plan, now)
    }
}
