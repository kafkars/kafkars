//! Bounded ownership and normalization of tracked Produce calls.
mod permit;
#[cfg(test)]
mod permit_test;
mod route_refresh;
#[cfg(test)]
mod route_refresh_test;
mod settlement;
mod settlement_normalize;
#[cfg(test)]
mod settlement_normalize_test;
#[cfg(test)]
mod settlement_test;
#[cfg(test)]
use kafka_client_core::{BatchExecutionId, ProducerInput};
use kafka_client_core::{Deadline, Moment};
use kafka_driver::RoutedCall;
use kafka_wire::ProduceResponse;

use super::produce_call_entries::TrackedProduceEntries;
use crate::clock::OperationDeadline;
pub(crate) use permit::ProduceCallPermit;
pub(crate) use route_refresh::ProduceRouteRefreshPoll;
pub(crate) use settlement::ProduceCompletionFailure;
#[cfg(test)]
pub(crate) use settlement::RecoveredProduceCall as RecoveredProduceCallForTest;
use settlement::{RecoveredProduceCall, SettledProduceCall};

struct TrackedProduceCall {
    entries: TrackedProduceEntries,
    broker_id: i32,
    deadline: OperationDeadline,
    call: RoutedCall<ProduceResponse>,
}

pub(crate) struct TrackedProduceCalls {
    capacity: usize,
    max_in_flight_requests_per_broker: usize,
    calls: Vec<TrackedProduceCall>,
    settled: Option<SettledProduceCall>,
    recovered: Vec<RecoveredProduceCall>,
}

impl TrackedProduceCalls {
    pub(crate) fn with_max_in_flight_requests_per_broker(
        capacity: usize,
        max_in_flight_requests_per_broker: usize,
    ) -> Self {
        Self {
            capacity,
            max_in_flight_requests_per_broker,
            calls: Vec::with_capacity(capacity),
            settled: None,
            recovered: Vec::with_capacity(capacity),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_submit_then_pending_refresh_for_test(
        execution: BatchExecutionId,
        deadline: OperationDeadline,
        input: ProducerInput,
    ) -> Self {
        Self {
            capacity: 1,
            max_in_flight_requests_per_broker: 5,
            calls: Vec::new(),
            settled: Some(
                SettledProduceCall::with_submit_then_pending_refresh_for_test(
                    execution, deadline, input,
                ),
            ),
            recovered: Vec::with_capacity(1),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_missing_route_refresh_for_test(
        execution: BatchExecutionId,
        deadline: OperationDeadline,
        input: ProducerInput,
    ) -> Self {
        Self {
            capacity: 1,
            max_in_flight_requests_per_broker: 5,
            calls: Vec::new(),
            settled: Some(SettledProduceCall::from_input(
                execution, deadline, input, None,
            )),
            recovered: Vec::with_capacity(1),
        }
    }

    #[cfg(test)]
    pub(crate) fn settle_first_as_transport_failure_for_test(&mut self, now: Moment) {
        let call = self.calls.remove(0);
        self.settled = Some(SettledProduceCall::from_tracked_failure_for_test(call, now));
    }

    /// Atomically reserves global ownership and one exact broker's request slot.
    pub(crate) fn try_reserve_for(&mut self, broker_id: i32) -> Option<ProduceCallPermit<'_>> {
        if self
            .calls
            .len()
            .saturating_add(usize::from(self.settled.is_some()))
            .saturating_add(self.recovered.len())
            >= self.capacity
            || !self.broker_admission_available(broker_id)
        {
            return None;
        }
        Some(ProduceCallPermit::from_reserved_exact_broker_lane(
            &mut self.calls,
            broker_id,
        ))
    }

    #[cfg(test)]
    pub(crate) fn try_reserve(&mut self) -> Option<ProduceCallPermit<'_>> {
        self.try_reserve_for(0)
    }

    /// Reports capacity under the configured exact per-broker request gate.
    pub(crate) fn broker_admission_available(&self, broker_id: i32) -> bool {
        self.broker_in_flight_request_count(broker_id) < self.max_in_flight_requests_per_broker
    }

    /// Returns transport-owned requests for one exact broker lane.
    pub(crate) fn broker_in_flight_request_count(&self, broker_id: i32) -> usize {
        self.calls
            .iter()
            .filter(|call| call.broker_id == broker_id)
            .count()
    }

    pub(crate) fn in_flight_request_count(&self) -> usize {
        self.calls.len()
    }

    pub(crate) fn retained_count(&self) -> usize {
        self.calls
            .len()
            .saturating_add(usize::from(self.settled.is_some()))
            .saturating_add(self.recovered.len())
    }

    pub(crate) fn next_refresh_deadline(&self) -> Option<Deadline> {
        self.settled.as_ref()?.refresh_deadline()
    }

    pub(crate) fn poll_next_ready(
        &mut self,
        now: Moment,
    ) -> Result<Option<&mut SettledProduceCall>, ProduceCompletionFailure> {
        if self.settled.is_some() {
            return Ok(self.settled.as_mut());
        }
        let Some((index, result)) = self
            .calls
            .iter()
            .enumerate()
            .find_map(|(index, call)| call.call.try_result().map(|result| (index, result)))
        else {
            return Ok(None);
        };
        let TrackedProduceCall {
            entries,
            broker_id: _completed_broker,
            deadline,
            call,
        } = self.calls.remove(index);
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(source) => return Err(ProduceCompletionFailure::new(entries, source)),
        };
        drop(call);
        let (result, _selected_version, route_token) = outcome.into_parts();
        self.settled = Some(SettledProduceCall::from_terminal(
            entries,
            deadline,
            result,
            now,
            route_token,
        ));
        Ok(self.settled.as_mut())
    }

    pub(crate) fn discard_settled(&mut self, now: Moment) {
        if self
            .settled
            .as_mut()
            .is_some_and(|settled| settled.advance(now))
        {
            return;
        }
        drop(self.settled.take());
    }

    pub(crate) fn recover_after_driver_shutdown(&mut self) {
        debug_assert!(self.recovered.is_empty());
        for call in self.calls.drain(..) {
            let TrackedProduceCall {
                entries,
                broker_id: _shutdown_broker,
                deadline: _shutdown_deadline,
                call,
            } = call;
            drop(call);
            self.recovered.push(RecoveredProduceCall::new(entries));
        }
        if let Some(settled) = self.settled.take() {
            self.recovered.push(settled.recover_after_driver_shutdown());
        }
    }

    pub(crate) fn seal_recovered_after_execution_unavailable(&mut self) {
        for recovered in self.recovered.drain(..) {
            recovered.seal();
        }
    }
}
