//! Bounded ownership and normalization of tracked Produce calls.

use std::{error::Error, fmt, sync::Arc};

use kafka_client_core::{BatchExecutionId, Moment, ProducerAttemptFailureKind, ProducerInput};
use kafka_driver::{CompletionError, RequestError, RouteFailureToken, RoutedCall};
use kafka_wire::ProduceResponse;

use crate::{
    clock::OperationDeadline,
    protocol::{
        produce::MaterializedProduce,
        produce_outcome::{explicit_produce_response_input, produce_transport_failure_input},
    },
};

use super::{super::DriverOwner, ProduceSubmitError};

/// One accepted driver call retaining its exact core and route correlation.
pub(crate) struct TrackedProduceCall {
    execution: BatchExecutionId,
    topic: Arc<str>,
    partition: i32,
    call: RoutedCall<ProduceResponse>,
}

impl TrackedProduceCall {
    fn new(
        execution: BatchExecutionId,
        topic: Arc<str>,
        partition: i32,
        call: RoutedCall<ProduceResponse>,
    ) -> Self {
        Self {
            execution,
            topic,
            partition,
            call,
        }
    }
}

/// Reservation proving a driver-accepted call can be retained without waiting.
pub(crate) struct ProduceCallPermit<'a> {
    calls: &'a mut Vec<TrackedProduceCall>,
}

impl ProduceCallPermit<'_> {
    /// Builds, submits, and retains one exact prepared owner.
    pub(crate) fn submit(
        self,
        driver: &DriverOwner,
        execution: BatchExecutionId,
        deadline: OperationDeadline,
        materialized: MaterializedProduce,
        now: Moment,
    ) -> Result<(), ProduceSubmitError> {
        let topic = materialized.topic_owner();
        let partition = materialized.partition();
        let request = materialized.into_name_routed_request(now, deadline);
        let call = driver.submit_tracked_produce(
            topic.as_ref(),
            partition,
            request,
            deadline.transport(),
        )?;
        self.calls
            .push(TrackedProduceCall::new(execution, topic, partition, call));
        Ok(())
    }
}

/// One terminal driver result retaining route authority through core application.
pub(crate) struct SettledProduceCall {
    input: ProducerInput,
    route_token: Option<RouteFailureToken>,
}

impl SettledProduceCall {
    /// Returns the normalized fact while retaining route authority.
    pub(crate) const fn input(&self) -> ProducerInput {
        self.input
    }

    fn discard(self) -> Option<RouteFailureToken> {
        self.route_token
    }
}

/// Driver completion ownership disappeared or was consumed out of band.
#[derive(Debug)]
pub(crate) struct ProduceCompletionFailure {
    execution: BatchExecutionId,
    source: CompletionError,
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

/// Bounded linear owner of all driver-accepted Produce calls.
pub(crate) struct TrackedProduceCalls {
    capacity: usize,
    calls: Vec<TrackedProduceCall>,
    settled: Option<SettledProduceCall>,
}

impl TrackedProduceCalls {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            calls: Vec::with_capacity(capacity),
            settled: None,
        }
    }

    /// Reserves one already-allocated slot before prepared bytes move.
    pub(crate) fn try_reserve(&mut self) -> Option<ProduceCallPermit<'_>> {
        if self
            .calls
            .len()
            .saturating_add(usize::from(self.settled.is_some()))
            >= self.capacity
        {
            return None;
        }
        Some(ProduceCallPermit {
            calls: &mut self.calls,
        })
    }

    /// Returns every pending or settled driver-owned call.
    pub(crate) fn retained_count(&self) -> usize {
        self.calls
            .len()
            .saturating_add(usize::from(self.settled.is_some()))
    }

    /// Polls bounded owners in admission order while retaining terminal authority.
    pub(crate) fn poll_next_ready(
        &mut self,
        now: Moment,
    ) -> Result<Option<&SettledProduceCall>, ProduceCompletionFailure> {
        if self.settled.is_some() {
            return Ok(self.settled.as_ref());
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
        let (result, route_token) = outcome.into_parts();
        let input = normalized_terminal_input(
            call.execution,
            call.topic.as_ref(),
            call.partition,
            now,
            &result,
        );
        self.settled = Some(SettledProduceCall { input, route_token });
        Ok(self.settled.as_ref())
    }

    /// Discards route authority only after core accepted the terminal fact.
    pub(crate) fn discard_settled(&mut self) {
        if let Some(settled) = self.settled.take() {
            let route_token = settled.discard();
            // Core does not yet authorize invalidation. A later routing-policy
            // join may consume this token instead of deliberately discarding it.
            drop(route_token);
        }
    }

    /// Drops all tracked observers only after the embedded driver is gone.
    pub(crate) fn discard_after_driver_shutdown(&mut self) {
        self.calls.clear();
        if let Some(settled) = self.settled.take() {
            drop(settled.discard());
        }
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
        Ok(response) => explicit_produce_response_input(execution, topic, partition, response)
            .unwrap_or_else(|failure| {
                produce_transport_failure_input(
                    execution,
                    now,
                    ProducerAttemptFailureKind::Permanent,
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
