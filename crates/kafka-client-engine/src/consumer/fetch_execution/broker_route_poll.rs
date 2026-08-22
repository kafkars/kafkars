//! Terminal polling and exact ownership recovery for metadata-routed Fetch calls.

use crate::driver::BrokerFetchRouteFailureKind;
use kafka_client_core::{AssignedConsumerMachine, AssignedConsumerTransition, FetchFailure};

use super::{
    broker_execution::RoutedBrokerFetch,
    executor::DirectFetchExecutor,
    fault::{FetchExecutionError, RetainedFetchFault},
    prepared::PreparedFetchExecution,
};

impl DirectFetchExecutor {
    pub(super) fn poll_one_broker_route(
        &mut self,
        machine: &mut AssignedConsumerMachine,
    ) -> Result<Option<AssignedConsumerTransition>, FetchExecutionError> {
        let Some((index, terminal)) = self
            .route_calls
            .iter_mut()
            .enumerate()
            .find_map(|(index, pending)| pending.call.try_terminal().map(|value| (index, value)))
        else {
            return Ok(None);
        };
        let pending = self.route_calls.swap_remove(index);
        match terminal {
            Ok(routed) => {
                let (request, broker_id) = routed.into_parts();
                self.routed.push(RoutedBrokerFetch {
                    broker_id,
                    request,
                    hard_output_bytes: pending.hard_output_bytes,
                });
                Ok(None)
            }
            Err(failure) => {
                let (request, kind) = failure.into_parts();
                let prepared =
                    PreparedFetchExecution::from_parts(request, pending.hard_output_bytes);
                match kind {
                    BrokerFetchRouteFailureKind::Terminal(FetchFailure::Transport) => {
                        match self.retain_topic_route_retry(prepared) {
                            Ok(()) => Ok(None),
                            Err(prepared) => {
                                match self.settle_unadmitted(
                                    machine,
                                    prepared,
                                    FetchFailure::Transport,
                                )? {
                                    super::FetchSubmission::Settled(transition) => Ok(transition),
                                    _ => unreachable!("terminal route fact settles immediately"),
                                }
                            }
                        }
                    }
                    BrokerFetchRouteFailureKind::Terminal(failure) => {
                        match self.settle_unadmitted(machine, prepared, failure)? {
                            super::FetchSubmission::Settled(transition) => Ok(transition),
                            _ => unreachable!("terminal route fact settles immediately"),
                        }
                    }
                    BrokerFetchRouteFailureKind::Backpressured
                    | BrokerFetchRouteFailureKind::Completion => {
                        self.fault = Some(RetainedFetchFault::Prepared {
                            _prepared: prepared,
                        });
                        Err(FetchExecutionError::BrokerRouteCompletion)
                    }
                }
            }
        }
    }
}
