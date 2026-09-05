//! Atomic KIP-951 Fetch replacement after exact broker leader-movement facts.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerInput, AssignedConsumerMachine,
    AssignedConsumerTransition, FetchFailure, Moment,
};

use crate::driver::{BrokerId, DriverOwner, FetchRouteRefresh};

use super::{
    super::assigned_event::AssignedConsumerEventStore,
    executor::DirectFetchExecutor,
    fault::{FetchExecutionError, RetainedFetchFault},
    prepared::PreparedFetchExecution,
    route_refresh::WaitingLeaderRoute,
    terminal::{FetchTerminalAction, TerminalStorage},
    terminal_proposal::{FetchTerminalProposal, LeaderMovementFetchProposal},
};

impl DirectFetchExecutor {
    pub(in crate::consumer) fn apply_terminal_proposal_with_driver(
        &mut self,
        driver: &DriverOwner,
        machine: &mut AssignedConsumerMachine,
        events: &mut AssignedConsumerEventStore,
        proposal: FetchTerminalProposal,
        now: Moment,
    ) -> Result<Option<AssignedConsumerTransition>, FetchExecutionError> {
        match proposal.into_leader_movement_retry() {
            Ok(proposal) => {
                self.apply_leader_movement_retry(driver, machine, events, proposal, now)
            }
            Err(proposal) => self.apply_terminal_proposal(machine, proposal),
        }
    }

    pub(super) fn drive_waiting_leader_route(
        &mut self,
        driver: &DriverOwner,
        machine: &mut AssignedConsumerMachine,
        waiting: WaitingLeaderRoute,
        now: Moment,
    ) -> Result<(Option<AssignedConsumerTransition>, bool), FetchExecutionError> {
        match waiting {
            WaitingLeaderRoute::Failed {
                prepared,
                hinted_broker: Some(broker_id),
            } => {
                if self.routed.len() >= self.route_capacity {
                    self.leader_recovery
                        .restore_waiting(WaitingLeaderRoute::Failed {
                            prepared,
                            hinted_broker: Some(broker_id),
                        });
                    return Ok((None, false));
                }
                self.restore_routed(broker_id, prepared);
                Ok((None, true))
            }
            WaitingLeaderRoute::Failed {
                prepared,
                hinted_broker: None,
            } => {
                let super::FetchSubmission::Settled(transition) =
                    self.settle_unadmitted(machine, prepared, FetchFailure::Transport)?
                else {
                    unreachable!("failed refresh settles its retained Fetch")
                };
                Ok((transition, true))
            }
            WaitingLeaderRoute::Ready {
                prepared,
                hinted_broker,
            } => {
                if let Some(broker_id) = hinted_broker {
                    if self.routed.len() >= self.route_capacity {
                        self.leader_recovery
                            .restore_waiting(WaitingLeaderRoute::Ready {
                                prepared,
                                hinted_broker: Some(broker_id),
                            });
                        return Ok((None, false));
                    }
                    self.restore_routed(broker_id, prepared);
                    return Ok((None, true));
                }
                match self.submit_broker_route(driver, machine, prepared, now)? {
                    super::FetchSubmission::Accepted => Ok((None, true)),
                    super::FetchSubmission::Settled(transition) => Ok((transition, true)),
                    super::FetchSubmission::Backpressured(prepared)
                    | super::FetchSubmission::Unavailable(prepared) => {
                        self.leader_recovery
                            .restore_waiting(WaitingLeaderRoute::Ready {
                                prepared,
                                hinted_broker: None,
                            });
                        Ok((None, false))
                    }
                }
            }
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one atomic retry transition preserves every request, event, store, route, and broker-session rollback owner"
    )]
    fn apply_leader_movement_retry(
        &mut self,
        _driver: &DriverOwner,
        machine: &mut AssignedConsumerMachine,
        events: &mut AssignedConsumerEventStore,
        proposal: LeaderMovementFetchProposal,
        now: Moment,
    ) -> Result<Option<AssignedConsumerTransition>, FetchExecutionError> {
        let clear_leader_epoch = proposal.clears_leader_epoch();
        let hinted_broker = proposal
            .leader()
            .and_then(|leader| BrokerId::from_raw(leader.broker_id).ok());
        if !self.leader_recovery.can_retain_attempt() {
            return self.apply_terminal_proposal(machine, proposal.into_proposal());
        }
        let transport_failure = proposal.is_transport();
        let (mut fact, leader) = proposal.into_parts();
        let fence = fact.request.fence();
        let active_broker = self
            .active_broker_sessions
            .iter()
            .find(|active| active.fences.contains(&fence))
            .map(|active| active.plan.broker_id());
        let retry_broker = hinted_broker.or(if clear_leader_epoch {
            active_broker
        } else {
            None
        });
        let route_available = retry_broker.is_some() && self.routed.len() < self.route_capacity;
        if fact.request.operation_deadline().core().is_elapsed_at(now) {
            fact.action = FetchTerminalAction::Apply(AssignedConsumerInput::FetchFailed {
                fence,
                failure: FetchFailure::DeadlineElapsed,
            });
            return self.apply_terminal_proposal(machine, FetchTerminalProposal::new(fact, None));
        }
        let event_claims = match events.prepare_partition(fence.position().partition()) {
            Ok(claims) => claims,
            Err(error) => {
                self.fault = Some(RetainedFetchFault::Request {
                    _request: fact.request,
                });
                return Err(FetchExecutionError::Event(error));
            }
        };
        let transition = match machine.apply(AssignedConsumerInput::FetchRetryAuthorized { fence })
        {
            Ok(transition) => transition,
            Err(error) => {
                event_claims.rollback_event_claims();
                self.fault = Some(RetainedFetchFault::Request {
                    _request: fact.request,
                });
                return Err(FetchExecutionError::Core(error));
            }
        };
        let [
            AssignedConsumerEffect::FetchReady {
                fence: replacement,
                next_offset,
            },
        ] = transition.effects()
        else {
            event_claims.rollback_event_claims();
            self.retain_transition(fact.request, transition);
            return Err(FetchExecutionError::UnexpectedRetryAuthorization { fence });
        };
        if replacement.position() != fence.position()
            || *next_offset != fact.request.next_offset()
            || replacement.revision() <= fence.revision()
        {
            event_claims.rollback_event_claims();
            self.retain_transition(fact.request, transition);
            return Err(FetchExecutionError::UnexpectedRetryAuthorization { fence });
        }
        if let Err(error) = event_claims.commit_event_claims(transition.effects()) {
            self.retain_transition(fact.request, transition);
            return Err(FetchExecutionError::Event(error));
        }
        match fact.storage {
            TerminalStorage::Released => {}
            TerminalStorage::NonDelivery(stored) if stored == fence => {
                if let Err(error) = self.store.discard_non_delivery(stored) {
                    self.retain_transition(fact.request, transition);
                    return Err(FetchExecutionError::Store(error));
                }
            }
            TerminalStorage::NonDelivery(_) | TerminalStorage::Deliverable(_, _) => {
                self.retain_transition(fact.request, transition);
                return Err(FetchExecutionError::UnexpectedRetryStorage { fence });
            }
        }
        let route_token = match self.broker_calls.confirm_fetch_retry(fence) {
            Ok(route_token) => route_token,
            Err(error) => {
                self.retain_transition(fact.request, transition);
                return Err(FetchExecutionError::Confirm(error));
            }
        };
        let refresh = FetchRouteRefresh::from_token(route_token);
        match self.complete_broker_session(fence, crate::protocol::fetch::FetchSessionUpdate::Reset)
        {
            Ok(true) => {}
            Ok(false) => {
                self.retain_transition(fact.request, transition);
                return Err(FetchExecutionError::BrokerSession);
            }
            Err(error) => {
                self.retain_transition(fact.request, transition);
                return Err(error);
            }
        }
        fact.request.bind_retry(
            *replacement,
            *next_offset,
            leader.map(|leader| leader.epoch),
        );
        if let (true, Some(broker_id)) = (transport_failure, active_broker) {
            fact.request.mark_failed_broker(broker_id);
        }
        if clear_leader_epoch {
            fact.request.clear_leader_epoch();
        }
        let prepared = PreparedFetchExecution::from_parts(fact.request, fact.hard_output_bytes);
        self.leader_recovery.begin(
            refresh,
            Some(prepared),
            retry_broker.filter(|_| route_available),
        );
        Ok(None)
    }
}
