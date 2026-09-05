//! Exact Fetch route admission fenced after one observed broker failure.

use kafka_client_core::{
    AssignedConsumerMachine, AssignedConsumerTransition, FetchFailure, Moment,
};

use crate::driver::{
    BrokerFetchCausalRouteFailure, BrokerFetchCausalRouteFailureKind, BrokerFetchRouteCall,
    BrokerRouteFailureToken, DriverOwner,
};

use super::{
    broker_execution::PendingBrokerRoute, executor::DirectFetchExecutor,
    fault::FetchExecutionError, prepared::PreparedFetchExecution,
    route_refresh::WaitingLeaderRoute,
};

impl DirectFetchExecutor {
    pub(super) fn drive_causal_leader_route(
        &mut self,
        driver: &DriverOwner,
        machine: &mut AssignedConsumerMachine,
        prepared: PreparedFetchExecution,
        failure_token: BrokerRouteFailureToken,
        now: Moment,
    ) -> Result<(Option<AssignedConsumerTransition>, bool), FetchExecutionError> {
        if !self.route_calls.is_empty()
            || self.route_calls.len().saturating_add(self.routed.len()) >= self.route_capacity
        {
            self.restore_causal_route(prepared, failure_token);
            return Ok((None, false));
        }
        if prepared.deadline().is_elapsed_at(now) {
            drop(failure_token);
            let super::FetchSubmission::Settled(transition) =
                self.settle_unadmitted(machine, prepared, FetchFailure::DeadlineElapsed)?
            else {
                unreachable!("elapsed causal route settles immediately")
            };
            return Ok((transition, true));
        }
        let (request, hard_output_bytes) = prepared.into_parts();
        let submission: Result<BrokerFetchRouteCall, BrokerFetchCausalRouteFailure> =
            BrokerFetchRouteCall::submit_after_failure(driver, request, failure_token);
        match submission {
            Ok(call) => {
                self.route_calls.push(PendingBrokerRoute {
                    call,
                    hard_output_bytes,
                });
                Ok((None, true))
            }
            Err(failure) => {
                let (request, kind) = failure.into_parts();
                let prepared = PreparedFetchExecution::from_parts(request, hard_output_bytes);
                match kind {
                    BrokerFetchCausalRouteFailureKind::Backpressured(failure_token) => {
                        self.restore_causal_route(prepared, failure_token);
                        Ok((None, false))
                    }
                    BrokerFetchCausalRouteFailureKind::Terminal(failure) => {
                        let super::FetchSubmission::Settled(transition) =
                            self.settle_unadmitted(machine, prepared, failure)?
                        else {
                            unreachable!("terminal causal route settles immediately")
                        };
                        Ok((transition, true))
                    }
                }
            }
        }
    }

    fn restore_causal_route(
        &mut self,
        prepared: PreparedFetchExecution,
        failure_token: BrokerRouteFailureToken,
    ) {
        self.leader_recovery
            .restore_waiting(WaitingLeaderRoute::Ready {
                prepared,
                hinted_broker: None,
                failure_token: Some(failure_token),
            });
    }
}
