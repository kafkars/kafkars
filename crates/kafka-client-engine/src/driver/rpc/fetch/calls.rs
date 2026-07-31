//! Bounded ownership, admission, control fencing, and polling of Fetch calls.

use kafka_client_core::{AssignedConsumerEffect, FetchFence, Moment};
use kafka_driver::RoutedCall;
use kafka_wire::FetchResponse as WireFetchResponse;

use super::{
    admission::{
        FetchAdmissionFailure, FetchCallAdmission, PartitionFetchRequest, submit_partition_fetch,
    },
    fence::supersedes,
    settlement::{FetchPoll, PendingFetchConfirmation, SettledFetchCall},
    stale::{FetchControlPending, StaleFetchDrains},
    terminal::{FetchCompletionFailure, FetchCompletionObservation, retain_fetch_terminal},
};
use crate::driver::DriverOwner;

pub(super) struct TrackedFetchCall {
    fence: FetchFence,
    pub(super) request: Option<PartitionFetchRequest>,
    call: RoutedCall<WireFetchResponse>,
}

impl TrackedFetchCall {
    fn mark_stale(&mut self, effect: AssignedConsumerEffect) -> Option<PartitionFetchRequest> {
        if !supersedes(effect, self.fence) {
            return None;
        }
        self.request.take()
    }
}

#[must_use = "a reserved Fetch-call slot must be submitted or released"]
struct FetchCallPermit<'a> {
    calls: &'a mut Vec<TrackedFetchCall>,
}

impl FetchCallPermit<'_> {
    #[allow(
        clippy::result_large_err,
        reason = "failed admission must return the exact linear prepared Fetch without allocation"
    )]
    fn submit(
        self,
        driver: &DriverOwner,
        request: PartitionFetchRequest,
        now: Moment,
    ) -> Result<(), FetchAdmissionFailure> {
        let accepted = submit_partition_fetch(driver, request, now)?;
        self.calls.push(TrackedFetchCall {
            fence: accepted.request.fence(),
            request: Some(accepted.request),
            call: accepted.call,
        });
        Ok(())
    }
}

/// Capacity-bounded registry of active, stale, settled, and confirming Fetch calls.
pub(crate) struct TrackedFetchCalls {
    pub(super) capacity: usize,
    pub(super) calls: Vec<TrackedFetchCall>,
    pub(super) settled: Option<SettledFetchCall>,
    pub(super) pending_confirmation: Option<PendingFetchConfirmation>,
    pub(super) completion_failure: Option<FetchCompletionFailure>,
}

impl TrackedFetchCalls {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            calls: Vec::with_capacity(capacity),
            settled: None,
            pending_confirmation: None,
            completion_failure: None,
        }
    }

    pub(crate) fn try_submit_fetch(
        &mut self,
        driver: &DriverOwner,
        request: PartitionFetchRequest,
        now: Moment,
    ) -> FetchCallAdmission {
        if request.operation_deadline().core().is_elapsed_at(now) {
            return FetchCallAdmission::Rejected(FetchAdmissionFailure::deadline_elapsed(request));
        }
        let Some(permit) = self.try_reserve() else {
            return FetchCallAdmission::Backpressured(request);
        };
        match permit.submit(driver, request, now) {
            Ok(()) => FetchCallAdmission::Accepted,
            Err(failure) => FetchCallAdmission::Rejected(failure),
        }
    }

    fn try_reserve(&mut self) -> Option<FetchCallPermit<'_>> {
        if self.retained_count() >= self.capacity {
            return None;
        }
        Some(FetchCallPermit {
            calls: &mut self.calls,
        })
    }

    pub(crate) fn retained_count(&self) -> usize {
        self.calls
            .len()
            .saturating_add(usize::from(self.settled.is_some()))
            .saturating_add(usize::from(self.pending_confirmation.is_some()))
            .saturating_add(usize::from(self.completion_failure.is_some()))
    }

    pub(crate) fn observe_fetch_control(
        &mut self,
        effect: AssignedConsumerEffect,
    ) -> Result<StaleFetchDrains, FetchControlPending> {
        if let Some(pending) = &self.pending_confirmation {
            return Err(FetchControlPending {
                fence: pending.fence(),
            });
        }
        let mut drains = StaleFetchDrains::new();
        for call in &mut self.calls {
            if let Some(request) = call.mark_stale(effect) {
                drains.push(request);
            }
        }
        if let Some(settled) = &mut self.settled {
            if let Some(request) = settled.mark_stale(effect) {
                drains.push(request);
            }
        }
        Ok(drains)
    }

    pub(crate) fn poll_fetch(
        &mut self,
        now: Moment,
    ) -> Result<FetchPoll, FetchCompletionObservation> {
        if let Some(failure) = &self.completion_failure {
            return Err(failure.observation());
        }
        if let Some(settled) = &self.settled {
            return Ok(settled_poll(settled));
        }
        let Some((index, result)) = self
            .calls
            .iter()
            .enumerate()
            .find_map(|(index, call)| call.call.try_result().map(|result| (index, result)))
        else {
            return Ok(FetchPoll::Idle);
        };
        let tracked = self.calls.remove(index);
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(source) => {
                let failure = FetchCompletionFailure::new(tracked.request, tracked.fence, source);
                let observation = failure.observation();
                self.completion_failure = Some(failure);
                return Err(observation);
            }
        };
        let (result, selected_version, route_token) = outcome.into_parts();
        self.settled = Some(match tracked.request {
            Some(request) => SettledFetchCall::live(
                retain_fetch_terminal(request, now, selected_version, result),
                route_token,
            ),
            None => SettledFetchCall::stale(tracked.fence, route_token),
        });
        Ok(self.settled.as_ref().map_or(FetchPoll::Idle, settled_poll))
    }

    #[cfg(test)]
    pub(crate) fn install_completion_failure_for_test(
        &mut self,
        request: PartitionFetchRequest,
        source: kafka_driver::CompletionError,
    ) {
        let fence = request.fence();
        self.completion_failure = Some(FetchCompletionFailure::new(Some(request), fence, source));
    }

    #[cfg(test)]
    pub(crate) fn install_consumed_completion_for_test(&mut self, request: PartitionFetchRequest) {
        self.install_completion_failure_for_test(request, kafka_driver::CompletionError::Consumed);
    }
}

fn settled_poll(settled: &SettledFetchCall) -> FetchPoll {
    if settled.is_stale() {
        FetchPoll::StaleConfirmationReady {
            fence: settled.fence(),
        }
    } else {
        FetchPoll::TerminalReady {
            fence: settled.fence(),
        }
    }
}
