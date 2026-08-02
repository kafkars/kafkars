//! Bounded ownership and normalization of tracked Produce calls.

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
use kafka_client_core::ProducerInput;
use kafka_client_core::{BatchExecutionId, Deadline, Moment};
use kafka_driver::RoutedCall;
use kafka_wire::ProduceResponse;

use crate::{clock::OperationDeadline, protocol::produce::MaterializedProduce};

use super::produce_call_entries::{TrackedProduceEntries, TrackedProduceEntry};
use super::{super::DriverOwner, ProduceSubmitError, produce_acceptance::AcceptedProduceCall};
pub(crate) use route_refresh::ProduceRouteRefreshPoll;
pub(crate) use settlement::ProduceCompletionFailure;
use settlement::{RecoveredProduceCall, SettledProduceCall};

pub(super) struct TrackedProduceCall {
    pub(super) entries: TrackedProduceEntries,
    pub(super) call: RoutedCall<ProduceResponse>,
}

pub(crate) struct ProduceCallPermit<'a> {
    pub(super) calls: &'a mut Vec<TrackedProduceCall>,
}

impl ProduceCallPermit<'_> {
    pub(crate) fn submit(
        self,
        driver: &DriverOwner,
        execution: BatchExecutionId,
        deadline: OperationDeadline,
        materialized: MaterializedProduce,
        now: Moment,
    ) -> Result<AcceptedProduceCall, ProduceSubmitError> {
        let topic = materialized.topic_owner();
        let partition = materialized.partition();
        let request = materialized.into_name_routed_request(now, deadline);
        let call = driver.submit_tracked_produce(
            topic.as_ref(),
            partition,
            request,
            deadline.transport(),
        )?;
        self.calls.push(TrackedProduceCall {
            entries: TrackedProduceEntries::Single(TrackedProduceEntry {
                execution,
                deadline: deadline.core(),
                topic,
                partition,
            }),
            call,
        });
        Ok(AcceptedProduceCall::new(execution))
    }
}

pub(crate) struct TrackedProduceCalls {
    capacity: usize,
    calls: Vec<TrackedProduceCall>,
    settled: Option<SettledProduceCall>,
    recovered: Vec<RecoveredProduceCall>,
}

impl TrackedProduceCalls {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            calls: Vec::with_capacity(capacity),
            settled: None,
            recovered: Vec::with_capacity(capacity),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_submit_then_pending_refresh_for_test(
        execution: BatchExecutionId,
        deadline: Deadline,
        input: ProducerInput,
    ) -> Self {
        Self {
            capacity: 1,
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
        deadline: Deadline,
        input: ProducerInput,
    ) -> Self {
        Self {
            capacity: 1,
            calls: Vec::new(),
            settled: Some(SettledProduceCall::from_input(
                execution, deadline, input, None,
            )),
            recovered: Vec::with_capacity(1),
        }
    }

    pub(crate) fn try_reserve(&mut self) -> Option<ProduceCallPermit<'_>> {
        if self
            .calls
            .len()
            .saturating_add(usize::from(self.settled.is_some()))
            .saturating_add(self.recovered.len())
            >= self.capacity
        {
            return None;
        }
        Some(ProduceCallPermit {
            calls: &mut self.calls,
        })
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
        let TrackedProduceCall { entries, call } = self.calls.remove(index);
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(source) => return Err(ProduceCompletionFailure::new(entries, source)),
        };
        drop(call);
        let (result, _selected_version, route_token) = outcome.into_parts();
        self.settled = Some(SettledProduceCall::from_terminal(
            entries,
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
            let TrackedProduceCall { entries, call } = call;
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

    #[cfg(test)]
    pub(crate) fn recovered(&self) -> &[RecoveredProduceCall] {
        &self.recovered
    }
}
