//! Allocation-free helpers shared by aggregate broker Fetch settlement stages.

use kafka_client_core::AssignedConsumerEffect;

use super::{
    admission::PartitionFetchRequest,
    broker_calls::{BrokerFetchSlot, SettledBrokerFetchBatch},
    fence::supersedes,
    settlement::FetchPoll,
    stale::StaleFetchDrains,
    terminal::FetchTerminal,
};

impl BrokerFetchSlot {
    pub(super) fn mark_stale(
        &mut self,
        effect: AssignedConsumerEffect,
    ) -> Option<PartitionFetchRequest> {
        if !supersedes(effect, self.fence) {
            return None;
        }
        self.request
            .take()
            .or_else(|| self.terminal.take().map(FetchTerminal::into_request))
    }

    pub(super) const fn is_stale(&self) -> bool {
        self.request.is_none() && self.terminal.is_none()
    }
}

pub(super) fn drain_stale(
    slots: &mut [BrokerFetchSlot],
    effect: AssignedConsumerEffect,
    drains: &mut StaleFetchDrains,
) {
    for slot in slots {
        if let Some(request) = slot.mark_stale(effect) {
            drains.push(request);
        }
    }
}

pub(super) fn take_slot_requests(slots: &mut [BrokerFetchSlot]) -> Vec<PartitionFetchRequest> {
    slots
        .iter_mut()
        .filter_map(|slot| {
            slot.request
                .take()
                .or_else(|| slot.terminal.take().map(FetchTerminal::into_request))
        })
        .collect()
}

pub(super) fn batch_poll(settled: &SettledBrokerFetchBatch) -> FetchPoll {
    let Some(slot) = settled.slots.first() else {
        return FetchPoll::Idle;
    };
    if slot.is_stale() {
        FetchPoll::StaleConfirmationReady { fence: slot.fence }
    } else {
        FetchPoll::TerminalReady { fence: slot.fence }
    }
}
