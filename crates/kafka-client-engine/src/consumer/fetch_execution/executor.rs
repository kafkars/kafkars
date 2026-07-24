//! Unique bounded owner joining Fetch calls, output reservations, and delivery.

use kafka_client_core::FetchFence;

use crate::driver::TrackedFetchCalls;

use super::{
    super::fetch_store::{FetchDeliveryStore, FetchStoreReservation},
    fault::RetainedFetchFault,
};

pub(super) struct ActiveFetchReservation {
    pub(super) fence: FetchFence,
    pub(super) reservation: FetchStoreReservation,
}

/// Concrete direct-assignment Fetch interpreter.
pub(crate) struct DirectFetchExecutor {
    _seal: ExecutorSeal,
    pub(super) calls: TrackedFetchCalls,
    pub(super) store: FetchDeliveryStore,
    pub(super) active: Vec<ActiveFetchReservation>,
    pub(super) fault: Option<RetainedFetchFault>,
}

struct ExecutorSeal;

impl DirectFetchExecutor {
    pub(crate) fn create_unbound(
        call_capacity: usize,
        delivery_capacity: usize,
        max_bytes: usize,
    ) -> Self {
        Self {
            _seal: ExecutorSeal,
            calls: TrackedFetchCalls::new(call_capacity),
            store: FetchDeliveryStore::new(delivery_capacity, max_bytes),
            active: Vec::new(),
            fault: None,
        }
    }

    pub(super) fn active_index(&self, fence: FetchFence) -> Option<usize> {
        self.active
            .iter()
            .position(|reservation| reservation.fence == fence)
    }

    pub(super) fn take_active(&mut self, index: usize) -> ActiveFetchReservation {
        self.active.swap_remove(index)
    }

    pub(crate) fn retained(&self) -> (usize, usize, usize) {
        let (deliveries, bytes) = self.store.retained();
        (self.calls.retained_count(), deliveries, bytes)
    }

    #[cfg(test)]
    pub(crate) fn reserve_output_for_test(
        &mut self,
        fence: FetchFence,
        bytes: usize,
    ) -> Result<(), super::super::fetch_store::FetchStoreFailure> {
        let reservation = self.store.try_reserve(fence, bytes)?;
        self.active
            .push(ActiveFetchReservation { fence, reservation });
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn tracked_calls_for_test(&mut self) -> &mut TrackedFetchCalls {
        &mut self.calls
    }

    #[cfg(test)]
    pub(crate) fn install_fault_for_test(&mut self) {
        self.fault = Some(RetainedFetchFault::Staged);
    }
}
