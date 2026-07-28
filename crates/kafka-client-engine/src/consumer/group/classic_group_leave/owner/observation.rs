//! Read-only leave ownership, close gate, and deadline observations.

use super::{ClassicGroupLeaveOwner, ClassicGroupLeaveState};

impl ClassicGroupLeaveOwner {
    pub(in crate::consumer::group) const fn owns_coordinator_invalidation(&self) -> bool {
        self.coordinator_invalidation_outstanding
    }

    pub(in crate::consumer::group) fn clear_coordinator_invalidation_after_driver_shutdown(
        &mut self,
    ) {
        self.coordinator_invalidation_outstanding = false;
    }

    pub(in crate::consumer::group) fn allows_local_close(&self) -> bool {
        !self.coordinator_invalidation_outstanding
            && matches!(
                self.state,
                ClassicGroupLeaveState::Dormant | ClassicGroupLeaveState::Terminal(_)
            )
    }

    pub(in crate::consumer::group) fn next_deadline(&self) -> Option<kafka_client_core::Deadline> {
        match &self.state {
            ClassicGroupLeaveState::Pending(deadline)
            | ClassicGroupLeaveState::RetryPending { deadline, .. }
            | ClassicGroupLeaveState::Prepared { deadline, .. }
            | ClassicGroupLeaveState::DriverOwned { deadline, .. }
            | ClassicGroupLeaveState::RediscoveryTransfer { deadline, .. }
            | ClassicGroupLeaveState::AwaitingInvalidation { deadline, .. }
            | ClassicGroupLeaveState::CompletionFault { deadline, .. } => Some(deadline.core()),
            ClassicGroupLeaveState::Dormant | ClassicGroupLeaveState::Terminal(_) => None,
        }
    }

    pub(in crate::consumer::group) fn unsettled(&self) -> usize {
        usize::from(!self.allows_local_close())
    }
}
