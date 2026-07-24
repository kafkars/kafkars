//! Delivery leasing and exact close quiescence for the assigned owner.

use kafka_client_core::{AssignedConsumerCloseId, AssignedConsumerInput};

use super::{
    assigned_close_error::AssignedCloseSlotPhase, assigned_owner::AssignedConsumerOwner,
    assigned_owner_fault::AssignedConsumerOwnerFault,
    assigned_owner_model::AssignedConsumerOwnerError, fetch_store::FetchDelivery,
};

impl AssignedConsumerOwner {
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

    /// Takes the sole retained close result.
    pub(crate) fn take_close(
        &mut self,
    ) -> Result<AssignedConsumerCloseId, AssignedConsumerOwnerError> {
        if self.is_faulted() {
            return Err(AssignedConsumerOwnerError::Faulted);
        }
        self.close
            .take_ready()
            .map_err(AssignedConsumerOwnerError::Close)
    }

    pub(super) fn progress_close(&mut self) -> bool {
        if self.is_faulted() || self.close.phase() != AssignedCloseSlotPhase::Accepted {
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

    fn is_quiescent(&self) -> bool {
        self.effects.is_empty()
            && self.raw_position_deadlines.is_empty()
            && self.pending_positions.is_empty()
            && self.pending_fetches.is_empty()
            && self.timers.timer_count() == 0
            && self.positions.retained_positions() == 0
            && self.fetches.retained() == (0, 0, 0)
            && !self.is_faulted()
    }

    fn retain_reclaim_failure(&mut self, failure: super::fetch_execution::FetchReclaimFailure) {
        if self.reclaim_faults.len() < self.limits.delivery_capacity {
            self.reclaim_faults.push(failure);
        } else {
            self.reclaim_overflow = Some(failure);
        }
    }
}
