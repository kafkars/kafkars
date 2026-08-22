//! Control fencing, polling, confirmation, and recovery for broker Fetch batches.

use kafka_client_core::{AssignedConsumerEffect, FetchFence, Moment};

use super::{
    broker_calls::{
        BrokerFetchCompletionFailure, PendingBrokerFetchConfirmation, SettledBrokerFetchBatch,
        TrackedBrokerFetchCalls,
    },
    broker_calls_helpers::{batch_poll, drain_stale, take_slot_requests},
    broker_calls_response::distribute_terminal,
    settlement::{
        FetchBeginSettlementError, FetchConfirmationError, FetchPoll, StaleFetchConfirmationError,
    },
    stale::{FetchControlPending, FetchRecovery, StaleFetchDrains},
    terminal::{FetchCompletionObservation, FetchTerminal, retain_fetch_terminal},
};

impl TrackedBrokerFetchCalls {
    pub(crate) fn observe_control(
        &mut self,
        effect: AssignedConsumerEffect,
    ) -> Result<StaleFetchDrains, FetchControlPending> {
        if let Some(pending) = &self.pending {
            return Err(FetchControlPending {
                fence: pending.fence,
            });
        }
        let mut drains = StaleFetchDrains::new();
        for call in &mut self.calls {
            drain_stale(&mut call.slots, effect, &mut drains);
        }
        if let Some(settled) = &mut self.settled {
            drain_stale(&mut settled.slots, effect, &mut drains);
        }
        Ok(drains)
    }

    pub(crate) fn poll_fetch(
        &mut self,
        now: Moment,
    ) -> Result<FetchPoll, FetchCompletionObservation> {
        if let Some(failure) = self.completion_failure.take() {
            if failure.source == kafka_driver::CompletionError::Closed {
                let mut slots = failure.slots;
                for slot in &mut slots {
                    if let Some(request) = slot.request.take() {
                        slot.terminal = Some(retain_fetch_terminal(
                            request,
                            now,
                            None,
                            Err(kafka_driver::RequestError::ConnectionClosed(
                                kafka_driver::ResponseCloseReason::TransportClosed,
                            )),
                        ));
                    }
                }
                self.settled = Some(SettledBrokerFetchBatch {
                    slots,
                    route_token: None,
                });
            } else {
                let observation = failure.observation;
                self.completion_failure = Some(failure);
                return Err(observation);
            }
        }
        if let Some(settled) = &self.settled {
            return Ok(batch_poll(settled));
        }
        let Some((index, result)) = self
            .calls
            .iter()
            .enumerate()
            .find_map(|(index, call)| call.call.try_result().map(|result| (index, result)))
        else {
            return Ok(FetchPoll::Idle);
        };
        let mut tracked = self.calls.remove(index);
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(source) => {
                let fence = tracked.slots.first().map_or_else(
                    || unreachable!("accepted broker Fetch is nonempty"),
                    |slot| slot.fence,
                );
                let observation = FetchCompletionObservation::from_driver(fence, source);
                self.completion_failure = Some(BrokerFetchCompletionFailure {
                    slots: tracked.slots,
                    observation,
                    source,
                });
                return Err(observation);
            }
        };
        let (result, selected_version, route_token) = outcome.into_parts();
        distribute_terminal(&mut tracked.slots, now, selected_version, result);
        self.settled = Some(SettledBrokerFetchBatch {
            slots: tracked.slots,
            route_token,
        });
        Ok(self.settled.as_ref().map_or(FetchPoll::Idle, batch_poll))
    }

    pub(crate) fn begin_fetch_settlement(
        &mut self,
        supplied: FetchFence,
    ) -> Result<FetchTerminal, FetchBeginSettlementError> {
        if let Some(pending) = &self.pending {
            return Err(FetchBeginSettlementError::ConfirmationPending {
                pending: pending.fence,
            });
        }
        let Some(settled) = &mut self.settled else {
            return Err(FetchBeginSettlementError::NoSettledCall { supplied });
        };
        let Some(slot) = settled.slots.iter_mut().find(|slot| slot.fence == supplied) else {
            let actual = settled.slots.first().map_or(supplied, |slot| slot.fence);
            return Err(FetchBeginSettlementError::FenceMismatch {
                settled: actual,
                supplied,
            });
        };
        let Some(terminal) = slot.terminal.take() else {
            return Err(FetchBeginSettlementError::StaleSettledCall { supplied });
        };
        self.pending = Some(PendingBrokerFetchConfirmation { fence: supplied });
        Ok(terminal)
    }

    pub(crate) fn confirm_fetch_settlement(
        &mut self,
        supplied: FetchFence,
    ) -> Result<(), FetchConfirmationError> {
        let Some(pending) = &self.pending else {
            return Err(FetchConfirmationError::NoPendingConfirmation { supplied });
        };
        if pending.fence != supplied {
            return Err(FetchConfirmationError::FenceMismatch {
                pending: pending.fence,
                supplied,
            });
        }
        self.pending = None;
        self.remove_settled_slot(supplied);
        Ok(())
    }

    pub(crate) fn confirm_fetch_retry(
        &mut self,
        supplied: FetchFence,
    ) -> Result<Option<kafka_driver::RouteFailureToken>, FetchConfirmationError> {
        self.validate_pending(supplied)?;
        let route_token = self
            .settled
            .as_mut()
            .and_then(|settled| settled.route_token.take());
        self.pending = None;
        self.remove_settled_slot(supplied);
        Ok(route_token)
    }

    pub(crate) fn confirm_stale_fetch(
        &mut self,
        supplied: FetchFence,
    ) -> Result<(), StaleFetchConfirmationError> {
        let Some(settled) = &self.settled else {
            return Err(StaleFetchConfirmationError::NoSettledCall { supplied });
        };
        let Some(slot) = settled.slots.iter().find(|slot| slot.fence == supplied) else {
            let actual = settled.slots.first().map_or(supplied, |slot| slot.fence);
            return Err(StaleFetchConfirmationError::FenceMismatch {
                settled: actual,
                supplied,
            });
        };
        if !slot.is_stale() {
            return Err(StaleFetchConfirmationError::LiveSettledCall { supplied });
        }
        self.remove_settled_slot(supplied);
        Ok(())
    }

    pub(crate) fn recover_after_driver_shutdown(&mut self) -> FetchRecovery {
        let mut requests = Vec::new();
        for call in &mut self.calls {
            requests.extend(take_slot_requests(&mut call.slots));
        }
        self.calls.clear();
        if let Some(mut settled) = self.settled.take() {
            requests.extend(take_slot_requests(&mut settled.slots));
        }
        self.pending = None;
        let observation = self.completion_failure.take().map(|mut failure| {
            requests.extend(take_slot_requests(&mut failure.slots));
            failure.observation
        });
        FetchRecovery::new(requests, observation)
    }

    fn remove_settled_slot(&mut self, fence: FetchFence) {
        let Some(settled) = &mut self.settled else {
            return;
        };
        if let Some(index) = settled.slots.iter().position(|slot| slot.fence == fence) {
            settled.slots.swap_remove(index);
        }
        if settled.slots.is_empty() {
            self.settled = None;
        }
    }

    fn validate_pending(&self, supplied: FetchFence) -> Result<(), FetchConfirmationError> {
        let Some(pending) = &self.pending else {
            return Err(FetchConfirmationError::NoPendingConfirmation { supplied });
        };
        if pending.fence != supplied {
            return Err(FetchConfirmationError::FenceMismatch {
                pending: pending.fence,
                supplied,
            });
        }
        Ok(())
    }
}
