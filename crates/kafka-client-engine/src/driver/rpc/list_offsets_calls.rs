//! Bounded ownership of fenced, partition-routed `ListOffsets` calls.

use kafka_client_core::{AssignedConsumerEffect, Moment, PositionFence};
use kafka_driver::{CompletionError, RouteFailureToken, RoutedCall};
use kafka_wire::ListOffsetsResponse;

use crate::protocol::consumer::ListOffsetsIsolation;

use super::{
    super::DriverOwner,
    list_offsets_admission::{
        PositionAdmissionFailure, PositionResolutionRequest, submit_position_request,
    },
    list_offsets_fence::supersedes,
    list_offsets_terminal::{PositionResolutionTerminal, normalize_position_terminal},
};

struct TrackedPositionCall {
    fence: PositionFence,
    topic: String,
    isolation: ListOffsetsIsolation,
    call: RoutedCall<ListOffsetsResponse>,
    stale: bool,
}

impl TrackedPositionCall {
    fn observe_control(&mut self, effect: AssignedConsumerEffect) {
        self.stale = self.stale || supersedes(effect, self.fence);
    }
}

/// Preflighted ownership of exactly one bounded call slot.
#[must_use = "a reserved position-call slot must be submitted or released"]
pub(crate) struct PositionCallPermit<'a> {
    calls: &'a mut Vec<TrackedPositionCall>,
}

impl PositionCallPermit<'_> {
    pub(crate) fn submit(
        self,
        driver: &DriverOwner,
        request: PositionResolutionRequest,
        now: Moment,
    ) -> Result<(), PositionAdmissionFailure> {
        let accepted = submit_position_request(driver, request, now)?;
        self.calls.push(TrackedPositionCall {
            fence: accepted.fence,
            topic: accepted.topic,
            isolation: accepted.isolation,
            call: accepted.call,
            stale: false,
        });
        Ok(())
    }
}

/// Terminal ownership retained until core accepts or fences the fact.
pub(crate) struct SettledPositionCall {
    terminal: PositionResolutionTerminal,
    route_token: Option<RouteFailureToken>,
    stale: bool,
}

impl SettledPositionCall {
    pub(crate) const fn terminal(&self) -> PositionResolutionTerminal {
        self.terminal
    }

    fn discard(self) {
        drop(self.route_token);
    }

    fn observe_control(&mut self, effect: AssignedConsumerEffect) {
        self.stale = self.stale || supersedes(effect, self.terminal.fence());
    }
}

/// Completion ownership failed independently of Kafka request semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PositionCompletionFailure {
    fence: PositionFence,
    source: CompletionError,
}

impl PositionCompletionFailure {
    #[cfg(test)]
    pub(crate) const fn fence(self) -> PositionFence {
        self.fence
    }

    #[cfg(test)]
    pub(crate) const fn source(self) -> CompletionError {
        self.source
    }

    #[cfg(test)]
    pub(crate) const fn is_consumed(self) -> bool {
        matches!(self.source, CompletionError::Consumed)
    }
}

/// Capacity-bounded registry of accepted position lookup calls.
pub(crate) struct TrackedPositionCalls {
    capacity: usize,
    calls: Vec<TrackedPositionCall>,
    settled: Option<SettledPositionCall>,
    completion_failure: Option<PositionCompletionFailure>,
}

impl TrackedPositionCalls {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            calls: Vec::with_capacity(capacity),
            settled: None,
            completion_failure: None,
        }
    }

    pub(crate) fn try_reserve(&mut self) -> Option<PositionCallPermit<'_>> {
        if self.retained_count() >= self.capacity {
            return None;
        }
        Some(PositionCallPermit {
            calls: &mut self.calls,
        })
    }

    pub(crate) fn retained_count(&self) -> usize {
        self.calls
            .len()
            .saturating_add(usize::from(self.settled.is_some()))
            .saturating_add(usize::from(self.completion_failure.is_some()))
    }

    pub(crate) fn observe_control(&mut self, effect: AssignedConsumerEffect) {
        for call in &mut self.calls {
            call.observe_control(effect);
        }
        if let Some(settled) = &mut self.settled {
            settled.observe_control(effect);
        }
    }

    pub(crate) fn poll_next_ready(
        &mut self,
        now: Moment,
    ) -> Result<Option<&mut SettledPositionCall>, PositionCompletionFailure> {
        if let Some(failure) = self.completion_failure {
            return Err(failure);
        }
        if self.settled.as_ref().is_some_and(|settled| settled.stale) {
            self.discard_settled();
        }
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
        let tracked = self.calls.remove(index);
        let (terminal, route_token) = match result {
            Ok(outcome) => {
                let (result, version, token) = outcome.into_parts();
                (
                    normalize_position_terminal(
                        tracked.fence,
                        &tracked.topic,
                        tracked.isolation,
                        now,
                        version,
                        result,
                    ),
                    token,
                )
            }
            Err(source) => {
                let failure = PositionCompletionFailure {
                    fence: tracked.fence,
                    source,
                };
                self.completion_failure = Some(failure);
                return Err(failure);
            }
        };
        if tracked.stale {
            drop(route_token);
            return Ok(None);
        }
        self.settled = Some(SettledPositionCall {
            terminal,
            route_token,
            stale: false,
        });
        Ok(self.settled.as_mut())
    }

    pub(crate) fn discard_settled(&mut self) {
        if let Some(settled) = self.settled.take() {
            settled.discard();
        }
    }

    /// Releases otherwise-unsettleable ownership only after the driver owner is gone.
    pub(crate) fn recover_positions_after_driver_shutdown(
        &mut self,
    ) -> Option<PositionCompletionFailure> {
        self.calls.clear();
        self.discard_settled();
        self.completion_failure.take()
    }

    #[cfg(test)]
    pub(crate) fn install_terminal_for_test(&mut self, fence: PositionFence, now: Moment) {
        self.settled = Some(SettledPositionCall {
            terminal: PositionResolutionTerminal::failed(fence, now),
            route_token: None,
            stale: false,
        });
    }

    #[cfg(test)]
    pub(crate) fn install_completion_failure_for_test(
        &mut self,
        fence: PositionFence,
        source: CompletionError,
    ) {
        self.completion_failure = Some(PositionCompletionFailure { fence, source });
    }

    #[cfg(test)]
    pub(crate) fn install_consumed_failure_for_test(&mut self, fence: PositionFence) {
        self.install_completion_failure_for_test(fence, CompletionError::Consumed);
    }
}
