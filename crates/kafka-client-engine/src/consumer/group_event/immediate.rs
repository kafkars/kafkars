//! Immediate observation through the unique classic-group capability.

use super::{GroupConsumerState, GroupConsumerStateError, GroupConsumerStateErrorKind};
use crate::consumer::{GroupConsumerHandle, GroupConsumerStatePortError};

impl GroupConsumerHandle {
    /// Immediately copies the current driver-confirmed membership and assignment.
    ///
    /// `Ok(None)` means no Sync-confirmed membership is current. This starts no
    /// group work, requests no reactor turn, and does not consume rebalance events.
    pub fn state(&self) -> Result<Option<GroupConsumerState>, GroupConsumerStateError> {
        match self.port.try_group_state(self.group_id) {
            Ok(state) => Ok(state),
            Err(error) if error.is_terminal() => Ok(None),
            Err(error) => Err(GroupConsumerStateError::new(match error {
                GroupConsumerStatePortError::Lock(error) if error.is_contended() => {
                    GroupConsumerStateErrorKind::Contended
                }
                GroupConsumerStatePortError::Lock(error) if error.is_poisoned() => {
                    GroupConsumerStateErrorKind::HostUnavailable
                }
                GroupConsumerStatePortError::Registry(error) if error.is_allocation() => {
                    GroupConsumerStateErrorKind::Allocation
                }
                GroupConsumerStatePortError::Closed
                | GroupConsumerStatePortError::Lock(_)
                | GroupConsumerStatePortError::Registry(_) => {
                    GroupConsumerStateErrorKind::InternalInvariant
                }
            })),
        }
    }
}
