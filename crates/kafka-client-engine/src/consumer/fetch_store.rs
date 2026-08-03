//! Bounded ownership of direct-consumer Fetch reservations and deliveries.

use kafka_client_core::FetchFence;

use crate::protocol::fetch::{
    FetchOutputReservation, FetchReservationDomain, RetainedFetchOutcome,
};

#[path = "fetch_store/batch.rs"]
mod batch;
#[path = "fetch_store/delivery.rs"]
mod delivery;
#[cfg(test)]
mod delivery_test;
#[path = "fetch_store/failure.rs"]
mod failure;
#[path = "fetch_store/outcome.rs"]
mod outcome;
#[cfg(test)]
mod outcome_test;

pub(crate) use delivery::FetchDelivery;
pub(crate) use failure::FetchStoreFailure;
pub(crate) use outcome::FetchStageKind;
use outcome::stage_kind;

/// Linear count-and-byte reservation acquired before driver admission.
#[must_use = "a Fetch reservation must be normalized or rolled back"]
pub(crate) struct FetchStoreReservation {
    proof: FetchStageProof,
    output: FetchOutputReservation,
}

impl FetchStoreReservation {
    pub(crate) fn into_protocol_parts(self) -> (FetchStageProof, FetchOutputReservation) {
        (self.proof, self.output)
    }
}

/// Linear store provenance retained while protocol normalization consumes capacity.
#[must_use = "a Fetch stage proof must be staged or rolled back"]
pub(crate) struct FetchStageProof {
    fence: FetchFence,
    reservation: FetchOutputReservation,
}

struct FetchSlot {
    sequence: u64,
    fence: FetchFence,
    charged_bytes: usize,
    provenance: Option<FetchOutputReservation>,
    outcome: Option<RetainedFetchOutcome>,
    state: SlotState,
}

impl FetchSlot {
    fn stage(&mut self, reservation: FetchOutputReservation, outcome: RetainedFetchOutcome) {
        self.charged_bytes = outcome.retained_bytes();
        self.provenance = Some(reservation);
        self.outcome = Some(outcome);
        self.state = SlotState::Staged;
    }

    fn authorize(&mut self, order: u64) {
        self.state = SlotState::Ready(order);
    }

    fn take_outcome(&mut self) -> Option<RetainedFetchOutcome> {
        self.outcome.take()
    }

    fn restore_outcome(&mut self, outcome: RetainedFetchOutcome) {
        self.outcome = Some(outcome);
    }

    fn lease(&mut self) {
        self.state = SlotState::Leased;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SlotState {
    Reserved,
    Staged,
    Ready(u64),
    Leased,
}

pub(crate) struct FetchDeliveryStore {
    domain: FetchReservationDomain,
    next_sequence: Option<u64>,
    next_authorization: Option<u64>,
    max_count: usize,
    max_bytes: usize,
    used_bytes: usize,
    slots: Vec<FetchSlot>,
}

impl FetchDeliveryStore {
    pub(crate) fn new(max_count: usize, max_bytes: usize) -> Self {
        Self {
            domain: FetchReservationDomain::create_store_domain(),
            next_sequence: Some(1),
            next_authorization: Some(1),
            max_count,
            max_bytes,
            used_bytes: 0,
            slots: Vec::new(),
        }
    }

    pub(crate) fn try_reserve(
        &mut self,
        fence: FetchFence,
        bytes: usize,
    ) -> Result<FetchStoreReservation, FetchStoreFailure> {
        if self.slots.iter().any(|slot| slot.fence == fence) {
            return Err(FetchStoreFailure::DuplicateFence);
        }
        if self.slots.len() >= self.max_count {
            return Err(FetchStoreFailure::CountCapacity);
        }
        let next = self
            .used_bytes
            .checked_add(bytes)
            .ok_or(FetchStoreFailure::AccountingOverflow)?;
        if next > self.max_bytes {
            return Err(FetchStoreFailure::ByteCapacity);
        }
        self.slots
            .try_reserve(1)
            .map_err(|_error| FetchStoreFailure::CountCapacity)?;
        let sequence = self
            .next_sequence
            .ok_or(FetchStoreFailure::ReservationIdentityExhausted)?;
        let (proof, output) = self.domain.issue_pair(sequence, bytes);
        self.slots.push(FetchSlot {
            sequence,
            fence,
            charged_bytes: bytes,
            provenance: None,
            outcome: None,
            state: SlotState::Reserved,
        });
        self.next_sequence = sequence.checked_add(1);
        self.used_bytes = next;
        Ok(FetchStoreReservation {
            proof: FetchStageProof {
                fence,
                reservation: proof,
            },
            output,
        })
    }

    pub(crate) fn rollback(
        &mut self,
        proof: FetchStageProof,
        output: FetchOutputReservation,
    ) -> Result<(), (FetchStoreFailure, (FetchStageProof, FetchOutputReservation))> {
        let index = match self.reserved(&proof) {
            Ok(index) if proof.reservation.same_reservation(&output) => index,
            Ok(_) => {
                return Err((FetchStoreFailure::ReservationMismatch, (proof, output)));
            }
            Err(error) => return Err((error, (proof, output))),
        };
        match self.remove(index) {
            Ok(()) => Ok(()),
            Err(error) => Err((error, (proof, output))),
        }
    }

    #[allow(
        clippy::result_large_err,
        reason = "corruption returns the intact outcome and linear proof"
    )]
    pub(crate) fn stage(
        &mut self,
        proof: FetchStageProof,
        outcome: RetainedFetchOutcome,
    ) -> Result<FetchStageKind, (FetchStoreFailure, (FetchStageProof, RetainedFetchOutcome))> {
        let index = match self.reserved(&proof) {
            Ok(index) if outcome.matches_reservation(&proof.reservation) => index,
            Ok(_) => {
                return Err((FetchStoreFailure::ReservationMismatch, (proof, outcome)));
            }
            Err(error) => return Err((error, (proof, outcome))),
        };
        let kind = match stage_kind(&outcome) {
            Ok(kind) => kind,
            Err(error) => return Err((error, (proof, outcome))),
        };
        let Some(next_used) = self.used_bytes.checked_sub(outcome.unused_reserved_bytes()) else {
            return Err((FetchStoreFailure::ReservationMismatch, (proof, outcome)));
        };
        let slot = &mut self.slots[index];
        slot.stage(proof.reservation, outcome);
        self.used_bytes = next_used;
        Ok(kind)
    }

    pub(crate) fn retained(&self) -> (usize, usize) {
        (self.slots.len(), self.used_bytes)
    }

    /// Reports whether one authorized delivery must drain before assignment fencing.
    pub(crate) fn has_ready(&self) -> bool {
        self.slots
            .iter()
            .any(|slot| matches!(slot.state, SlotState::Ready(_)))
    }

    fn reserved(&self, proof: &FetchStageProof) -> Result<usize, FetchStoreFailure> {
        let index = self.index(proof.fence)?;
        let slot = &self.slots[index];
        if slot.provenance.is_some() || slot.outcome.is_some() || slot.state != SlotState::Reserved
        {
            return Err(FetchStoreFailure::InvalidState);
        }
        if !proof.reservation.same_domain(&self.domain)
            || proof.reservation.sequence() != slot.sequence
            || slot.charged_bytes != proof.reservation.bytes()
        {
            return Err(FetchStoreFailure::ReservationMismatch);
        }
        Ok(index)
    }

    fn index(&self, fence: FetchFence) -> Result<usize, FetchStoreFailure> {
        self.slots
            .iter()
            .position(|slot| slot.fence == fence)
            .ok_or(FetchStoreFailure::UnknownFence)
    }

    fn remove(&mut self, index: usize) -> Result<(), FetchStoreFailure> {
        let next = self
            .used_bytes
            .checked_sub(self.slots[index].charged_bytes)
            .ok_or(FetchStoreFailure::ReservationMismatch)?;
        let _slot = self.slots.swap_remove(index);
        self.used_bytes = next;
        Ok(())
    }
}
