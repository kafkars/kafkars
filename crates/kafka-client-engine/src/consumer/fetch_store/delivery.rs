//! FIFO authorization and linear reclamation of retained Fetch deliveries.

use kafka_client_core::{FetchFence, NextFetchOffset};

use crate::protocol::fetch::RetainedFetchOutcome;

use super::{
    FetchDeliveryStore, FetchSlot, FetchStageKind, FetchStoreFailure, SlotState, outcome::slot_kind,
};

/// Linear application ownership while the store retains count and byte charges.
#[must_use = "a fetched delivery must be explicitly reclaimed"]
pub(crate) struct FetchDelivery {
    fence: FetchFence,
    next_offset: NextFetchOffset,
    outcome: RetainedFetchOutcome,
}

impl FetchDelivery {
    pub(crate) const fn fence(&self) -> FetchFence {
        self.fence
    }

    pub(crate) const fn next_offset(&self) -> NextFetchOffset {
        self.next_offset
    }

    pub(crate) const fn outcome(&self) -> &RetainedFetchOutcome {
        &self.outcome
    }
}

impl FetchDeliveryStore {
    pub(crate) fn authorize(
        &mut self,
        fence: FetchFence,
        next_offset: NextFetchOffset,
    ) -> Result<(), FetchStoreFailure> {
        let index = self.index(fence)?;
        let slot = &mut self.slots[index];
        if slot.state != SlotState::Staged {
            return Err(FetchStoreFailure::InvalidState);
        }
        match slot_kind(slot)? {
            FetchStageKind::Progress(actual, _) | FetchStageKind::Deliverable(actual, _)
                if actual == next_offset => {}
            FetchStageKind::Progress(_, _) | FetchStageKind::Deliverable(_, _) => {
                return Err(FetchStoreFailure::NextOffsetMismatch);
            }
            _ => return Err(FetchStoreFailure::NotDeliverable),
        }
        let order = self
            .next_authorization
            .ok_or(FetchStoreFailure::AuthorizationIdentityExhausted)?;
        self.next_authorization = order.checked_add(1);
        slot.authorize(order);
        Ok(())
    }

    pub(crate) fn discard_non_delivery(
        &mut self,
        fence: FetchFence,
    ) -> Result<(), FetchStoreFailure> {
        let index = self.index(fence)?;
        let slot = &self.slots[index];
        if slot.state != SlotState::Staged {
            return Err(FetchStoreFailure::InvalidState);
        }
        if matches!(
            slot_kind(slot)?,
            FetchStageKind::Progress(_, _) | FetchStageKind::Deliverable(_, _)
        ) {
            return Err(FetchStoreFailure::NotDeliverable);
        }
        self.remove(index)
    }

    pub(crate) fn discard_stale(&mut self, fence: FetchFence) -> Result<(), FetchStoreFailure> {
        let index = self.index(fence)?;
        if self.slots[index].state != SlotState::Staged {
            return Err(FetchStoreFailure::InvalidState);
        }
        self.remove(index)
    }

    pub(crate) fn take_ready(&mut self) -> Result<Option<FetchDelivery>, FetchStoreFailure> {
        let Some(index) = self
            .slots
            .iter()
            .enumerate()
            .filter_map(|(index, slot)| match slot.state {
                SlotState::Ready(order) => Some((order, index)),
                _ => None,
            })
            .min_by_key(|(order, _)| *order)
            .map(|(_, index)| index)
        else {
            return Ok(None);
        };
        let fence = self.slots[index].fence;
        let (next_offset, outcome) = take(&mut self.slots[index])?;
        Ok(Some(FetchDelivery {
            fence,
            next_offset,
            outcome,
        }))
    }

    #[allow(
        clippy::result_large_err,
        reason = "corruption returns the exact application-owned delivery"
    )]
    pub(crate) fn reclaim(
        &mut self,
        delivery: FetchDelivery,
    ) -> Result<(), (FetchStoreFailure, FetchDelivery)> {
        let index = match self.index(delivery.fence) {
            Ok(index) => index,
            Err(error) => return Err((error, delivery)),
        };
        let slot = &self.slots[index];
        if slot.state != SlotState::Leased
            || slot.charged_bytes != delivery.outcome.retained_bytes()
            || !slot
                .provenance
                .as_ref()
                .is_some_and(|proof| delivery.outcome.matches_reservation(proof))
        {
            return Err((FetchStoreFailure::ReservationMismatch, delivery));
        }
        match self.remove(index) {
            Ok(()) => Ok(()),
            Err(error) => Err((error, delivery)),
        }
    }
}

fn take(
    slot: &mut FetchSlot,
) -> Result<(NextFetchOffset, RetainedFetchOutcome), FetchStoreFailure> {
    if !matches!(slot.state, SlotState::Ready(_)) {
        return Err(FetchStoreFailure::InvalidState);
    }
    let outcome = slot.take_outcome().ok_or(FetchStoreFailure::InvalidState)?;
    let kind = match super::outcome::stage_kind(&outcome) {
        Ok(kind) => kind,
        Err(error) => {
            slot.restore_outcome(outcome);
            return Err(error);
        }
    };
    let next_offset = match kind {
        FetchStageKind::Progress(next_offset, _) | FetchStageKind::Deliverable(next_offset, _) => {
            next_offset
        }
        FetchStageKind::BrokerFailure(_) | FetchStageKind::Empty(_, _) => {
            slot.restore_outcome(outcome);
            return Err(FetchStoreFailure::NotDeliverable);
        }
    };
    slot.lease();
    Ok((next_offset, outcome))
}
