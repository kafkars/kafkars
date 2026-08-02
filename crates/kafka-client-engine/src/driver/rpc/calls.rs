//! Bounded ownership and normalization of tracked Produce calls.

use std::{error::Error, fmt, mem, sync::Arc};

use kafka_client_core::{
    BatchExecutionId, Deadline, DeliveryStatus, Moment, ProducerAttemptFailureKind,
    ProducerBrokerFailureKind, ProducerInput,
};
use kafka_driver::{
    Call, CompletionError, InvalidationDisposition, RequestError, RouteFailureToken, RouteKind,
    RoutedCall, SubmitError,
};
use kafka_wire::ProduceResponse;

use crate::{
    clock::OperationDeadline,
    protocol::{
        produce::MaterializedProduce,
        produce_outcome::{explicit_produce_response_input, produce_transport_failure_input},
    },
};

use super::{super::DriverOwner, ProduceSubmitError, produce_acceptance::AcceptedProduceCall};

pub(crate) struct TrackedProduceCall {
    execution: BatchExecutionId,
    deadline: Deadline,
    topic: Arc<str>,
    partition: i32,
    call: RoutedCall<ProduceResponse>,
}

impl TrackedProduceCall {
    fn new(
        execution: BatchExecutionId,
        deadline: Deadline,
        topic: Arc<str>,
        partition: i32,
        call: RoutedCall<ProduceResponse>,
    ) -> Self {
        Self {
            execution,
            deadline,
            topic,
            partition,
            call,
        }
    }
}

/// Raw accepted terminal retaining exact execution and route evidence.
#[must_use = "a raw Produce terminal owns unsettled execution and route evidence"]
pub(crate) struct ProduceRawTerminal {
    execution: BatchExecutionId,
    deadline: Deadline,
    topic: Arc<str>,
    partition: i32,
    result: Result<ProduceResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
}

impl ProduceRawTerminal {
    fn into_settled(self, now: Moment) -> SettledProduceCall {
        let input = normalized_terminal_input(
            self.execution,
            self.topic.as_ref(),
            self.partition,
            now,
            &self.result,
        );
        SettledProduceCall::new(self.execution, self.deadline, input, self.route_token)
    }
}

/// Reservation proving a driver-accepted call can be retained without waiting.
pub(crate) struct ProduceCallPermit<'a> {
    calls: &'a mut Vec<TrackedProduceCall>,
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
        self.calls.push(TrackedProduceCall::new(
            execution,
            deadline.core(),
            topic,
            partition,
            call,
        ));
        Ok(AcceptedProduceCall::new(execution))
    }
}

pub(crate) struct SettledProduceCall {
    execution: BatchExecutionId,
    deadline: Deadline,
    input: ProducerInput,
    _route_token: Option<RouteFailureToken>,
    route_refresh: ProduceRouteRefresh,
}

enum ProduceRouteRefresh {
    None,
    Unavailable,
    Queued(RouteFailureToken),
    Rejected(RouteFailureToken),
    Active(Call<InvalidationDisposition>),
    #[cfg(test)]
    SubmitForTest,
    #[cfg(test)]
    PendingForTest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProduceRouteRefreshPoll {
    Ready,
    Failed,
    Submitted,
    Pending,
}

impl SettledProduceCall {
    fn new(
        execution: BatchExecutionId,
        deadline: Deadline,
        input: ProducerInput,
        mut route_token: Option<RouteFailureToken>,
    ) -> Self {
        let route_refresh = if needs_partition_refresh(input) {
            if route_token.as_ref().map(RouteFailureToken::kind) == Some(RouteKind::PartitionLeader)
            {
                route_token.take().map_or(
                    ProduceRouteRefresh::Unavailable,
                    ProduceRouteRefresh::Queued,
                )
            } else {
                ProduceRouteRefresh::Unavailable
            }
        } else {
            ProduceRouteRefresh::None
        };
        Self {
            execution,
            deadline,
            input,
            _route_token: route_token,
            route_refresh,
        }
    }

    pub(crate) const fn input(&self) -> ProducerInput {
        self.input
    }

    pub(crate) fn poll_route_refresh(
        &mut self,
        driver: &DriverOwner,
        now: Moment,
    ) -> ProduceRouteRefreshPoll {
        if self
            .refresh_deadline()
            .is_some_and(|deadline| deadline.is_elapsed_at(now))
        {
            let delivery = match self.input {
                ProducerInput::BrokerFailed { delivery, .. }
                | ProducerInput::TransportFailed { delivery, .. } => delivery,
                _ => DeliveryStatus::PossiblySent,
            };
            self.input = ProducerInput::RouteRefreshDeadlineElapsed {
                execution: self.execution,
                now,
                delivery,
            };
            return ProduceRouteRefreshPoll::Ready;
        }
        match mem::replace(&mut self.route_refresh, ProduceRouteRefresh::None) {
            ProduceRouteRefresh::None => ProduceRouteRefreshPoll::Ready,
            ProduceRouteRefresh::Unavailable => ProduceRouteRefreshPoll::Failed,
            ProduceRouteRefresh::Rejected(route_token) => {
                self.route_refresh = ProduceRouteRefresh::Rejected(route_token);
                ProduceRouteRefreshPoll::Failed
            }
            ProduceRouteRefresh::Queued(route_token) => {
                match driver.driver.invalidate(route_token) {
                    Ok(call) => {
                        self.route_refresh = ProduceRouteRefresh::Active(call);
                        return ProduceRouteRefreshPoll::Submitted;
                    }
                    Err(rejection) => {
                        let retryable = invalidation_rejection_is_retryable(rejection.reason());
                        let (_source, route_token) = rejection.into_parts();
                        if retryable {
                            self.route_refresh = ProduceRouteRefresh::Queued(route_token);
                        } else {
                            self.route_refresh = ProduceRouteRefresh::Rejected(route_token);
                            return ProduceRouteRefreshPoll::Failed;
                        }
                    }
                }
                ProduceRouteRefreshPoll::Pending
            }
            ProduceRouteRefresh::Active(call) => match call.try_result() {
                Some(Ok(disposition)) if invalidation_disposition_allows_retry(disposition) => {
                    mark_route_refreshed(&mut self.input);
                    ProduceRouteRefreshPoll::Ready
                }
                Some(Ok(_) | Err(_)) => ProduceRouteRefreshPoll::Failed,
                None => {
                    self.route_refresh = ProduceRouteRefresh::Active(call);
                    ProduceRouteRefreshPoll::Pending
                }
            },
            #[cfg(test)]
            ProduceRouteRefresh::SubmitForTest => {
                self.route_refresh = ProduceRouteRefresh::PendingForTest;
                ProduceRouteRefreshPoll::Submitted
            }
            #[cfg(test)]
            ProduceRouteRefresh::PendingForTest => {
                self.route_refresh = ProduceRouteRefresh::PendingForTest;
                ProduceRouteRefreshPoll::Pending
            }
        }
    }

    fn refresh_deadline(&self) -> Option<Deadline> {
        match &self.route_refresh {
            ProduceRouteRefresh::Queued(_) | ProduceRouteRefresh::Active(_) => Some(self.deadline),
            #[cfg(test)]
            ProduceRouteRefresh::SubmitForTest | ProduceRouteRefresh::PendingForTest => {
                Some(self.deadline)
            }
            ProduceRouteRefresh::None
            | ProduceRouteRefresh::Unavailable
            | ProduceRouteRefresh::Rejected(_) => None,
        }
    }

    fn discard(self) {
        drop(self);
    }

    fn recover_after_driver_shutdown(self) -> RecoveredProduceCall {
        let execution = self.execution;
        drop(self);
        RecoveredProduceCall::new(execution)
    }
}

pub(super) fn needs_partition_refresh(input: ProducerInput) -> bool {
    matches!(
        input,
        ProducerInput::BrokerFailed { failure, .. }
            if failure.kind() == ProducerBrokerFailureKind::Routing
    ) || matches!(
        input,
        ProducerInput::TransportFailed {
            failure,
            route_refreshed: false,
            ..
        } if failure.is_structurally_transient()
    )
}

pub(super) fn mark_route_refreshed(input: &mut ProducerInput) {
    match input {
        ProducerInput::BrokerFailed {
            route_refreshed, ..
        }
        | ProducerInput::TransportFailed {
            route_refreshed, ..
        } => *route_refreshed = true,
        _ => {}
    }
}

pub(super) const fn invalidation_rejection_is_retryable(reason: &SubmitError) -> bool {
    matches!(reason, SubmitError::Full)
}

pub(super) const fn invalidation_disposition_allows_retry(
    disposition: InvalidationDisposition,
) -> bool {
    matches!(
        disposition,
        InvalidationDisposition::Applied | InvalidationDisposition::IgnoredStale
    )
}

#[derive(Debug)]
pub(crate) struct ProduceCompletionFailure {
    execution: BatchExecutionId,
    source: CompletionError,
}

#[must_use = "recovered Produce ownership must be sealed after producer settlement"]
pub(crate) struct RecoveredProduceCall {
    execution: BatchExecutionId,
}

impl RecoveredProduceCall {
    const fn new(execution: BatchExecutionId) -> Self {
        Self { execution }
    }

    #[cfg(test)]
    pub(crate) const fn execution(&self) -> BatchExecutionId {
        self.execution
    }

    fn seal(self) {
        debug_assert_ne!(self.execution.generation().get(), 0);
    }
}

impl fmt::Display for ProduceCompletionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "tracked Produce completion failed for batch {} generation {}: {}",
            self.execution.batch_id().get(),
            self.execution.generation().get(),
            self.source
        )
    }
}

impl Error for ProduceCompletionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
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
            settled: Some(SettledProduceCall {
                execution,
                deadline,
                input,
                _route_token: None,
                route_refresh: ProduceRouteRefresh::SubmitForTest,
            }),
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
            settled: Some(SettledProduceCall::new(execution, deadline, input, None)),
            recovered: Vec::with_capacity(1),
        }
    }
    /// Reserves one already-allocated slot before prepared bytes move.
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
        let call = self.calls.remove(index);
        let outcome = result.map_err(|source| ProduceCompletionFailure {
            execution: call.execution,
            source,
        })?;
        let (result, _selected_version, route_token) = outcome.into_parts();
        let raw = ProduceRawTerminal {
            execution: call.execution,
            deadline: call.deadline,
            topic: call.topic,
            partition: call.partition,
            result,
            route_token,
        };
        self.settled = Some(raw.into_settled(now));
        Ok(self.settled.as_mut())
    }

    pub(crate) fn discard_settled(&mut self) {
        if let Some(settled) = self.settled.take() {
            settled.discard();
        }
    }

    pub(crate) fn recover_after_driver_shutdown(&mut self) {
        debug_assert!(self.recovered.is_empty());
        for call in self.calls.drain(..) {
            let execution = call.execution;
            drop(call);
            self.recovered.push(RecoveredProduceCall::new(execution));
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

pub(super) fn normalized_terminal_input(
    execution: BatchExecutionId,
    topic: &str,
    partition: i32,
    now: Moment,
    result: &Result<ProduceResponse, RequestError>,
) -> ProducerInput {
    match result {
        Ok(response) => explicit_produce_response_input(execution, now, topic, partition, response)
            .unwrap_or_else(|failure| {
                produce_transport_failure_input(
                    execution,
                    now,
                    ProducerAttemptFailureKind::InvalidResponse,
                    failure.delivery(),
                )
            }),
        Err(error) => produce_transport_failure_input(
            execution,
            now,
            super::super::request_failure_kind(error),
            super::super::request_failure_delivery(error),
        ),
    }
}
