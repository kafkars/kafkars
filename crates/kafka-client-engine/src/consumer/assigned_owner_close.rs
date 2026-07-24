//! Delivery leasing, close quiescence, and exact terminal publication.

mod admission;
#[cfg(test)]
mod admission_test;

use kafka_client_core::AssignedConsumerInput;

use crate::completion::CompletionRegistryError;

use super::{
    assigned_close_error::AssignedCloseSlotPhase, assigned_host::AssignedConsumerCloseTerminal,
    assigned_host::AssignedConsumerDelivery, assigned_owner::AssignedConsumerOwner,
    assigned_owner_fault::AssignedConsumerOwnerFault,
    assigned_owner_model::AssignedConsumerOwnerError, fetch_store::FetchDelivery,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AssignedConsumerCloseSettlement {
    Idle,
    Published,
    Retry,
}

impl AssignedConsumerOwner {
    /// Joins an active Fetch lease with its catalog-owned public identity.
    pub(crate) fn take_named_delivery(
        &mut self,
    ) -> Result<Option<AssignedConsumerDelivery>, AssignedConsumerOwnerError> {
        let Some(delivery) = self.take_delivery()? else {
            return Ok(None);
        };
        let partition = delivery.fence().position().partition();
        let topic = match self.topics.name(partition.topic_id()) {
            Ok(topic) => std::sync::Arc::clone(topic),
            Err(error) => {
                self.fault = Some(AssignedConsumerOwnerFault::DeliveryTopic { error, delivery });
                return Err(AssignedConsumerOwnerError::Faulted);
            }
        };
        Ok(Some(AssignedConsumerDelivery::new(
            topic,
            partition.partition().get().cast_signed(),
            delivery,
        )))
    }

    /// Transfers one authorized delivery lease to the application boundary.
    pub(crate) fn take_delivery(
        &mut self,
    ) -> Result<Option<FetchDelivery>, AssignedConsumerOwnerError> {
        if self.is_faulted() {
            return Err(AssignedConsumerOwnerError::Faulted);
        }
        if !self.effects.is_empty() || self.close.phase() != AssignedCloseSlotPhase::Vacant {
            return Err(AssignedConsumerOwnerError::DeliveryUnavailable);
        }
        for _attempt in 0..self.limits.delivery_capacity {
            let delivery = self.fetches.take_ready().map_err(|error| {
                self.fault = Some(AssignedConsumerOwnerFault::Fetch(error));
                AssignedConsumerOwnerError::Faulted
            })?;
            let Some(delivery) = delivery else {
                return Ok(None);
            };
            match self.machine.delivery_ownership(delivery.fence()) {
                Ok(kafka_client_core::DeliveryOwnership::Active) => return Ok(Some(delivery)),
                Ok(kafka_client_core::DeliveryOwnership::Superseded) => {
                    self.reclaim_delivery(delivery)?;
                }
                Err(error) => {
                    self.fault = Some(AssignedConsumerOwnerFault::Delivery { error, delivery });
                    return Err(AssignedConsumerOwnerError::Faulted);
                }
            }
        }
        Ok(None)
    }

    /// Returns one application delivery lease and releases its retained charges.
    pub(crate) fn reclaim_delivery(
        &mut self,
        delivery: FetchDelivery,
    ) -> Result<(), AssignedConsumerOwnerError> {
        self.fetches.reclaim(delivery).map_err(|failure| {
            self.retain_reclaim_failure(failure);
            AssignedConsumerOwnerError::Faulted
        })
    }

    pub(super) fn progress_close(&mut self) -> bool {
        if self.is_faulted() {
            return false;
        }
        if self.close.phase() == AssignedCloseSlotPhase::Ready {
            return self.publish_normal_close_terminal();
        }
        if self.close.phase() != AssignedCloseSlotPhase::Accepted {
            return false;
        }
        match self.fetches.take_ready() {
            Ok(Some(delivery)) => {
                if let Err(failure) = self.fetches.reclaim(delivery) {
                    self.retain_reclaim_failure(failure);
                }
                return true;
            }
            Ok(None) => {}
            Err(error) => {
                self.fault = Some(AssignedConsumerOwnerFault::Fetch(error));
                return false;
            }
        }
        if !self.is_quiescent() {
            return false;
        }
        let close_id = match self.close.accepted_id() {
            Ok(close_id) => close_id,
            Err(error) => {
                self.fault = Some(AssignedConsumerOwnerFault::Close(error));
                return false;
            }
        };
        let input = AssignedConsumerInput::CloseDrained { close_id };
        match self.machine.apply(input) {
            Ok(transition) => {
                self.enqueue_transition(transition, None);
                true
            }
            Err(error) => {
                self.fault = Some(AssignedConsumerOwnerFault::Core {
                    input: AssignedConsumerInput::CloseDrained { close_id },
                    error,
                });
                false
            }
        }
    }

    /// Settles an accepted observer only after unique driver ownership is gone.
    pub(crate) fn settle_close_after_driver_shutdown(
        &mut self,
    ) -> Result<AssignedConsumerCloseSettlement, CompletionRegistryError> {
        let Some((completion_id, terminal)) = self.close.recovery_terminal() else {
            return Ok(AssignedConsumerCloseSettlement::Idle);
        };
        match self.publish_close(completion_id, terminal) {
            Ok(()) => Ok(AssignedConsumerCloseSettlement::Published),
            Err(CompletionRegistryError::NotificationBackpressure) => {
                Ok(AssignedConsumerCloseSettlement::Retry)
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn is_quiescent(&self) -> bool {
        self.effects.is_empty()
            && self.raw_position_deadlines.is_empty()
            && self.pending_positions.is_empty()
            && self.pending_fetches.is_empty()
            && self.timers.timer_count() == 0
            && self.positions.retained_positions() == 0
            && self.fetches.retained() == (0, 0, 0)
            && self.events.retained().0 == 0
            && !self.is_faulted()
    }

    fn retain_reclaim_failure(&mut self, failure: super::fetch_execution::FetchReclaimFailure) {
        if self.reclaim_faults.len() < self.limits.delivery_capacity {
            self.reclaim_faults.push(failure);
        } else {
            self.reclaim_overflow = Some(failure);
        }
    }

    fn publish_normal_close_terminal(&mut self) -> bool {
        let (completion_id, terminal) = match self.close.ready_terminal() {
            Ok(terminal) => terminal,
            Err(error) => {
                self.fault = Some(AssignedConsumerOwnerFault::Close(error));
                return false;
            }
        };
        match self.publish_close(completion_id, terminal) {
            Ok(()) => true,
            Err(CompletionRegistryError::NotificationBackpressure) => false,
            Err(error) => {
                self.fault = Some(AssignedConsumerOwnerFault::CloseCompletion(error));
                false
            }
        }
    }

    fn publish_close(
        &mut self,
        completion_id: crate::completion::CompletionId,
        terminal: AssignedConsumerCloseTerminal,
    ) -> Result<(), CompletionRegistryError> {
        #[cfg(test)]
        if let Some(error) = self.close_publish_faults.pop_front() {
            return Err(error);
        }
        self.close_completions
            .publish(completion_id, terminal)
            .map_err(|(error, _terminal)| error)?;
        self.close.mark_published(completion_id).map_err(|error| {
            self.fault = Some(AssignedConsumerOwnerFault::Close(error));
            CompletionRegistryError::UnknownCompletion
        })
    }

    #[cfg(test)]
    pub(crate) fn inject_close_publish_fault(&mut self, error: CompletionRegistryError) {
        self.close_publish_faults.push_back(error);
    }
}
