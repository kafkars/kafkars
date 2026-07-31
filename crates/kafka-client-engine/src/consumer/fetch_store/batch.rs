//! Atomic multi-partition reservation for one broker Fetch batch.

use kafka_client_core::FetchFence;

use super::{
    FetchDeliveryStore, FetchSlot, FetchStageProof, FetchStoreFailure, FetchStoreReservation,
    SlotState,
};

impl FetchDeliveryStore {
    pub(crate) fn try_reserve_batch(
        &mut self,
        requested: &[(FetchFence, usize)],
    ) -> Result<Vec<FetchStoreReservation>, FetchStoreFailure> {
        if requested
            .iter()
            .enumerate()
            .any(|(index, (fence, _bytes))| {
                self.slots.iter().any(|slot| slot.fence == *fence)
                    || requested[..index]
                        .iter()
                        .any(|(previous, _bytes)| previous == fence)
            })
        {
            return Err(FetchStoreFailure::DuplicateFence);
        }
        if self.slots.len().saturating_add(requested.len()) > self.max_count {
            return Err(FetchStoreFailure::CountCapacity);
        }
        let next_bytes = requested
            .iter()
            .try_fold(self.used_bytes, |used, (_fence, bytes)| {
                used.checked_add(*bytes)
                    .ok_or(FetchStoreFailure::AccountingOverflow)
            })?;
        if next_bytes > self.max_bytes {
            return Err(FetchStoreFailure::ByteCapacity);
        }
        let start = self
            .next_sequence
            .ok_or(FetchStoreFailure::ReservationIdentityExhausted)?;
        let count = u64::try_from(requested.len())
            .map_err(|_error| FetchStoreFailure::ReservationIdentityExhausted)?;
        let last = if requested.is_empty() {
            start
        } else {
            start
                .checked_add(count.saturating_sub(1))
                .ok_or(FetchStoreFailure::ReservationIdentityExhausted)?
        };
        self.slots
            .try_reserve_exact(requested.len())
            .map_err(|_error| FetchStoreFailure::CountCapacity)?;
        let mut reservations = Vec::new();
        reservations
            .try_reserve_exact(requested.len())
            .map_err(|_error| FetchStoreFailure::CountCapacity)?;
        for (index, (fence, bytes)) in requested.iter().copied().enumerate() {
            let offset = u64::try_from(index)
                .unwrap_or_else(|_| unreachable!("reserved batch index fits u64"));
            let sequence = start
                .checked_add(offset)
                .unwrap_or_else(|| unreachable!("batch sequence range was preflighted"));
            let (proof, output) = self.domain.issue_pair(sequence, bytes);
            self.slots.push(FetchSlot {
                sequence,
                fence,
                charged_bytes: bytes,
                provenance: None,
                outcome: None,
                state: SlotState::Reserved,
            });
            reservations.push(FetchStoreReservation {
                proof: FetchStageProof {
                    fence,
                    reservation: proof,
                },
                output,
            });
        }
        if !requested.is_empty() {
            self.next_sequence = last.checked_add(1);
        }
        self.used_bytes = next_bytes;
        Ok(reservations)
    }
}
