//! Exact control fencing and stale output-reservation release.

use kafka_client_core::AssignedConsumerEffect;

use super::{
    executor::DirectFetchExecutor,
    fault::{FetchExecutionError, RetainedFetchFault},
};

impl DirectFetchExecutor {
    pub(crate) fn observe_control(
        &mut self,
        effect: AssignedConsumerEffect,
    ) -> Result<(), FetchExecutionError> {
        if self.fault.is_some() {
            return Err(FetchExecutionError::Faulted);
        }
        let drains = if self.broker_calls_are_active() {
            self.broker_calls.observe_control(effect)
        } else {
            self.calls.observe_fetch_control(effect)
        }
        .map_err(FetchExecutionError::ControlPending)?;
        let mut requests = drains.into_requests();
        while let Some(request) = requests.pop() {
            let fence = request.fence();
            let Some(index) = self.active_index(fence) else {
                requests.push(request);
                self.fault = Some(RetainedFetchFault::ControlRequests {
                    _requests: requests,
                });
                return Err(FetchExecutionError::MissingReservation { fence });
            };
            let reservation = self.take_active(index).reservation;
            let (proof, output) = reservation.into_protocol_parts();
            if let Err((error, (proof, output))) = self.store.rollback(proof, output) {
                requests.push(request);
                self.fault = Some(RetainedFetchFault::ControlRollback {
                    _requests: requests,
                    _proof: proof,
                    _output: output,
                });
                return Err(FetchExecutionError::Store(error));
            }
        }
        self.retire_broker_routes_for_control(effect);
        if let Some(sessions) = &mut self.broker_sessions {
            sessions.observe_control(effect);
        }
        self.reset_fetch_session_for_control(effect);
        Ok(())
    }

    fn retire_broker_routes_for_control(&mut self, effect: AssignedConsumerEffect) {
        let mut pending = self.route_calls.len();
        while pending > 0 {
            pending -= 1;
            if self.route_calls[pending].call.is_superseded_by(effect) {
                let retired = self.route_calls.swap_remove(pending);
                drop(retired.call.retire_for_control());
            }
        }
        self.routed
            .retain(|routed| !routed.request.is_superseded_by(effect));
    }
}
