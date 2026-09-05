//! Retained leader-route recovery dispatch under the original Fetch deadline.

use kafka_client_core::{
    AssignedConsumerMachine, AssignedConsumerTransition, FetchFailure, Moment,
};

use crate::driver::DriverOwner;

use super::{
    executor::DirectFetchExecutor, fault::FetchExecutionError, route_refresh::WaitingLeaderRoute,
};

impl DirectFetchExecutor {
    pub(super) fn drive_waiting_leader_route(
        &mut self,
        driver: &DriverOwner,
        machine: &mut AssignedConsumerMachine,
        waiting: WaitingLeaderRoute,
        now: Moment,
    ) -> Result<(Option<AssignedConsumerTransition>, bool), FetchExecutionError> {
        match waiting {
            WaitingLeaderRoute::Ready {
                prepared,
                hinted_broker: _,
                failure_token: Some(failure_token),
            } => self.drive_causal_leader_route(driver, machine, prepared, failure_token, now),
            WaitingLeaderRoute::Failed {
                prepared,
                hinted_broker: Some(broker_id),
                failure_token,
            } => {
                if self.routed.len() >= self.route_capacity {
                    self.leader_recovery
                        .restore_waiting(WaitingLeaderRoute::Failed {
                            prepared,
                            hinted_broker: Some(broker_id),
                            failure_token,
                        });
                    return Ok((None, false));
                }
                self.restore_routed(broker_id, prepared);
                Ok((None, true))
            }
            WaitingLeaderRoute::Failed {
                prepared,
                hinted_broker: None,
                failure_token,
            } => {
                drop(failure_token);
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
                failure_token: None,
            } => {
                if let Some(broker_id) = hinted_broker {
                    if self.routed.len() >= self.route_capacity {
                        self.leader_recovery
                            .restore_waiting(WaitingLeaderRoute::Ready {
                                prepared,
                                hinted_broker: Some(broker_id),
                                failure_token: None,
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
                                failure_token: None,
                            });
                        Ok((None, false))
                    }
                }
            }
        }
    }
}
