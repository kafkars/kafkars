//! One bounded classic-group Fetch activation or execution action per registry turn.

use crate::{clock::MonotonicClock, driver::DriverOwner};

use super::{
    classic_group_entry_fault::ClassicGroupEntryFault,
    classic_group_fetch::{ClassicGroupFetchTransferTurn, transfer_completed_position},
    registry::GroupConsumerRegistry,
};

/// Result of driving the private Fetch owner for every retained group once.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum GroupConsumerFetchTurn {
    Idle,
    Progress,
    Blocked,
}

/// Stable hosted Fetch failure with linear detail retained by its entry owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum GroupConsumerFetchError {
    MissingOwnerFault,
}

impl GroupConsumerRegistry {
    pub(in crate::consumer::group) fn turn_fetch(
        &mut self,
        clock: &MonotonicClock,
        driver: &DriverOwner,
    ) -> Result<GroupConsumerFetchTurn, GroupConsumerFetchError> {
        for entry in &mut self.entries {
            if entry.fault.is_some() {
                continue;
            }
            match transfer_completed_position(
                &entry.classic,
                &entry.catalog,
                &mut entry.position,
                &mut entry.fetch,
            ) {
                Ok(ClassicGroupFetchTransferTurn::Activated) => {
                    return Ok(GroupConsumerFetchTurn::Progress);
                }
                Ok(ClassicGroupFetchTransferTurn::Idle) => {}
                Err(error) => {
                    entry.fault = Some(ClassicGroupEntryFault::FetchTransfer(error));
                    return Ok(GroupConsumerFetchTurn::Progress);
                }
            }
        }

        let mut blocked = false;
        for entry in &mut self.entries {
            if entry.fault.is_some() {
                continue;
            }
            if let Some(fault) = entry.fetch.fault() {
                entry.fault = Some(ClassicGroupEntryFault::FetchOwner(fault.kind()));
                return Ok(GroupConsumerFetchTurn::Progress);
            }
            let turn = entry.fetch.turn(&entry.catalog, clock, driver);
            if turn.fault_retained() {
                let Some(kind) = entry
                    .fetch
                    .fault()
                    .map(super::classic_group_fetch::ClassicGroupFetchOwnerFault::kind)
                else {
                    return Err(GroupConsumerFetchError::MissingOwnerFault);
                };
                entry.fault = Some(ClassicGroupEntryFault::FetchOwner(kind));
                return Ok(GroupConsumerFetchTurn::Progress);
            }
            if turn.progressed() {
                return Ok(GroupConsumerFetchTurn::Progress);
            }
            blocked |= turn.blocked();
        }
        Ok(if blocked {
            GroupConsumerFetchTurn::Blocked
        } else {
            GroupConsumerFetchTurn::Idle
        })
    }

    pub(in crate::consumer::group) fn fetch_unsettled(&self) -> usize {
        self.entries
            .iter()
            .map(|entry| entry.fetch.unsettled())
            .sum()
    }

    pub(in crate::consumer::group) fn fetch_next_deadline(
        &self,
    ) -> Option<kafka_client_core::Deadline> {
        self.entries
            .iter()
            .filter(|entry| entry.fault.is_none())
            .filter_map(|entry| entry.fetch.next_deadline())
            .min()
    }
}
