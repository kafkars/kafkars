//! Exact per-execution settlement, route refresh, and shutdown recovery for Produce calls.

use std::{error::Error, fmt, sync::Arc};

#[cfg(test)]
use kafka_client_core::BatchExecutionId;
use kafka_client_core::{Deadline, Moment, ProducerInput};
use kafka_driver::{CompletionError, RequestError, RouteFailureToken, RouteKind};
use kafka_wire::ProduceResponse;

use crate::{
    driver::DriverOwner,
    protocol::produce_response_batch::{
        BatchedProduceResponseIndex, validate_batched_produce_response,
    },
};

use super::super::produce_call_entries::TrackedProduceEntries;
use super::route_refresh::{ProduceRouteRefresh, ProduceRouteRefreshPoll, needs_route_refresh};
use super::settlement_normalize::normalized_entry_input;

#[cfg(test)]
mod test_support;

pub(crate) struct SettledProduceCall {
    entries: TrackedProduceEntries,
    result: Result<ProduceResponse, RequestError>,
    response_shape_valid: bool,
    response_index: Option<BatchedProduceResponseIndex>,
    shared_deadline: Deadline,
    input: ProducerInput,
    _route_token: Option<RouteFailureToken>,
    route_refresh: ProduceRouteRefresh,
}

impl SettledProduceCall {
    pub(super) fn from_terminal(
        entries: TrackedProduceEntries,
        result: Result<ProduceResponse, RequestError>,
        now: Moment,
        mut route_token: Option<RouteFailureToken>,
    ) -> Self {
        let shared_deadline = entries.first().deadline;
        let response_index = match &result {
            Ok(response) if entries.len() > 1 => validate_batched_produce_response(
                response,
                entries.len(),
                entries
                    .iter()
                    .map(|entry| (Arc::clone(&entry.topic), entry.partition)),
            )
            .ok(),
            _ => None,
        };
        let response_shape_valid =
            response_index.is_some() || (result.is_err() && entries.len() > 1);
        let input = normalized_entry_input(
            entries.first(),
            now,
            &result,
            entries.len() > 1,
            response_shape_valid,
            response_index.as_ref(),
        );
        // One name-route receipt proves only the selected routing partition.
        // Batch settlement therefore retains that token until drop and never
        // reports it as refresh evidence for every aggregated entry.
        let route_refresh_required = entries.len() == 1 && needs_route_refresh(input);
        let route_refresh = ProduceRouteRefresh::from_required(
            route_refresh_required,
            RouteKind::PartitionLeader,
            &mut route_token,
        );
        Self {
            entries,
            result,
            response_shape_valid,
            response_index,
            shared_deadline,
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
        self.route_refresh.poll(
            driver,
            self.shared_deadline,
            self.entries.first().execution,
            &mut self.input,
            now,
        )
    }

    pub(super) fn refresh_deadline(&self) -> Option<Deadline> {
        self.route_refresh.deadline(self.shared_deadline)
    }

    #[cfg(test)]
    pub(super) const fn route_refresh_required_for_test(&self) -> bool {
        !matches!(self.route_refresh, ProduceRouteRefresh::None)
    }

    pub(super) fn advance(&mut self, now: Moment) -> bool {
        if !self.entries.advance() {
            return false;
        }
        self.input = normalized_entry_input(
            self.entries.first(),
            now,
            &self.result,
            true,
            self.response_shape_valid,
            self.response_index.as_ref(),
        );
        true
    }

    pub(super) fn recover_after_driver_shutdown(self) -> RecoveredProduceCall {
        RecoveredProduceCall::new(self.entries)
    }
}

#[derive(Debug)]
pub(crate) struct ProduceCompletionFailure {
    entries: TrackedProduceEntries,
    source: CompletionError,
}

impl ProduceCompletionFailure {
    pub(super) const fn new(entries: TrackedProduceEntries, source: CompletionError) -> Self {
        Self { entries, source }
    }
}

impl fmt::Display for ProduceCompletionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "tracked Produce completion failed for batch {} generation {}: {}",
            self.entries.first().execution.batch_id().get(),
            self.entries.first().execution.generation().get(),
            self.source
        )
    }
}

impl Error for ProduceCompletionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

#[must_use = "recovered Produce ownership must be sealed after producer settlement"]
pub(crate) struct RecoveredProduceCall {
    entries: TrackedProduceEntries,
}

impl RecoveredProduceCall {
    pub(super) const fn new(entries: TrackedProduceEntries) -> Self {
        Self { entries }
    }

    #[cfg(test)]
    pub(crate) fn execution(&self) -> BatchExecutionId {
        self.entries.first().execution
    }

    #[cfg(test)]
    pub(crate) fn executions(&self) -> impl Iterator<Item = BatchExecutionId> + '_ {
        self.entries.iter().map(|entry| entry.execution)
    }

    pub(super) fn seal(self) {
        for entry in self.entries.iter() {
            debug_assert_ne!(entry.execution.generation().get(), 0);
        }
    }
}
