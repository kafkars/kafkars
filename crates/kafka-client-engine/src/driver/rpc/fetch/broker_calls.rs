//! Bounded admission and aggregate ownership for exact-broker Fetch calls.

use kafka_client_core::{FetchFence, Moment};
use kafka_driver::{BrokerId, CompletionError, RouteFailureToken, RoutedCall};
use kafka_wire::FetchResponse as WireFetchResponse;

use super::{
    admission::{FetchAdmissionFailureSource, PartitionFetchRequest},
    broker_admission::{BrokerFetchAdmissionFailure, submit_broker_fetch_batch},
    broker_calls_response::reserved_responses,
    terminal::{FetchCompletionObservation, FetchTerminal},
};
use crate::{driver::DriverOwner, protocol::fetch::ForgottenFetchPartition};

pub(super) struct BrokerFetchSlot {
    pub(super) fence: FetchFence,
    pub(super) request: Option<PartitionFetchRequest>,
    pub(super) response: WireFetchResponse,
    pub(super) terminal: Option<FetchTerminal>,
}

pub(super) struct TrackedBrokerFetchCall {
    pub(super) slots: Vec<BrokerFetchSlot>,
    pub(super) call: RoutedCall<WireFetchResponse>,
}

pub(super) struct SettledBrokerFetchBatch {
    pub(super) slots: Vec<BrokerFetchSlot>,
    pub(super) route_token: Option<RouteFailureToken>,
}

pub(super) struct PendingBrokerFetchConfirmation {
    pub(super) fence: FetchFence,
}

pub(super) struct BrokerFetchCompletionFailure {
    pub(super) requests: Vec<PartitionFetchRequest>,
    pub(super) observation: FetchCompletionObservation,
    pub(super) _source: CompletionError,
}

/// Result of one capacity-preflighted aggregate broker Fetch admission.
#[must_use = "backpressured or rejected broker Fetch ownership must be handled"]
pub(crate) enum BrokerFetchCallAdmission {
    Accepted,
    Backpressured(Vec<PartitionFetchRequest>),
    Rejected(BrokerFetchAdmissionFailure),
}

/// Capacity-bounded registry of aggregate broker Fetch calls and their terminals.
pub(crate) struct TrackedBrokerFetchCalls {
    pub(super) capacity: usize,
    pub(super) calls: Vec<TrackedBrokerFetchCall>,
    pub(super) settled: Option<SettledBrokerFetchBatch>,
    pub(super) pending: Option<PendingBrokerFetchConfirmation>,
    pub(super) completion_failure: Option<BrokerFetchCompletionFailure>,
}

impl TrackedBrokerFetchCalls {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            calls: Vec::with_capacity(capacity),
            settled: None,
            pending: None,
            completion_failure: None,
        }
    }

    pub(crate) fn try_submit(
        &mut self,
        driver: &DriverOwner,
        broker_id: BrokerId,
        requests: Vec<PartitionFetchRequest>,
        forgotten: &[ForgottenFetchPartition<'_>],
        now: Moment,
    ) -> BrokerFetchCallAdmission {
        if requests
            .iter()
            .any(|request| request.operation_deadline().core().is_elapsed_at(now))
        {
            return BrokerFetchCallAdmission::Rejected(BrokerFetchAdmissionFailure::new(
                requests,
                FetchAdmissionFailureSource::DeadlineElapsed,
            ));
        }
        if self.retained_count() >= self.capacity {
            return BrokerFetchCallAdmission::Backpressured(requests);
        }
        let responses = match reserved_responses(&requests) {
            Ok(responses) => responses,
            Err(source) => {
                return BrokerFetchCallAdmission::Rejected(BrokerFetchAdmissionFailure::new(
                    requests, source,
                ));
            }
        };
        let accepted = match submit_broker_fetch_batch(driver, broker_id, requests, forgotten, now)
        {
            Ok(accepted) => accepted,
            Err(failure) => return BrokerFetchCallAdmission::Rejected(failure),
        };
        let slots = accepted
            .requests
            .into_iter()
            .zip(responses)
            .map(|(request, response)| BrokerFetchSlot {
                fence: request.fence(),
                request: Some(request),
                response,
                terminal: None,
            })
            .collect();
        self.calls.push(TrackedBrokerFetchCall {
            slots,
            call: accepted.call,
        });
        BrokerFetchCallAdmission::Accepted
    }

    pub(crate) fn retained_count(&self) -> usize {
        self.calls
            .len()
            .saturating_add(usize::from(self.settled.is_some()))
            .saturating_add(usize::from(self.pending.is_some()))
            .saturating_add(usize::from(self.completion_failure.is_some()))
    }
}
