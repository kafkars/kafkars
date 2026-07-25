//! Linear composition of one core Join owner and its accepted driver receipt.

use crate::driver::classic_group::AcceptedJoinGroupCall;

use super::classic_group_join::{
    ClassicGroupJoinDriverAcceptance, ClassicGroupJoinIdentity, ClassicGroupJoinIntegrationOwner,
    ClassicGroupJoinTracking,
};

/// Exact local and driver ownership retained while Join is unsettled.
#[must_use = "an accepted classic Join must reach semantic and route settlement"]
pub(super) struct ClassicGroupJoinCallOwner {
    integration_for_join_call: ClassicGroupJoinIntegrationOwner,
    tracking_for_join_call: ClassicGroupJoinTracking,
    accepted_join_call_receipt: AcceptedJoinGroupCall,
}

/// Failed local handoff confirmation retaining both linear inputs.
#[must_use = "a failed Join handoff still owns the accepted driver receipt"]
pub(super) struct ClassicGroupJoinAcceptanceFailure {
    rejected_join_acceptance: ClassicGroupJoinDriverAcceptance,
    unrestored_join_receipt: AcceptedJoinGroupCall,
}

impl ClassicGroupJoinAcceptanceFailure {
    pub(super) const fn new(
        acceptance: ClassicGroupJoinDriverAcceptance,
        accepted: AcceptedJoinGroupCall,
    ) -> Self {
        Self {
            rejected_join_acceptance: acceptance,
            unrestored_join_receipt: accepted,
        }
    }

    pub(super) const fn identity(&self) -> ClassicGroupJoinIdentity {
        self.rejected_join_acceptance.identity()
    }

    pub(super) fn into_parts(self) -> (ClassicGroupJoinDriverAcceptance, AcceptedJoinGroupCall) {
        (self.rejected_join_acceptance, self.unrestored_join_receipt)
    }
}

impl ClassicGroupJoinCallOwner {
    pub(super) const fn new(
        integration: ClassicGroupJoinIntegrationOwner,
        tracking: ClassicGroupJoinTracking,
        accepted: AcceptedJoinGroupCall,
    ) -> Self {
        Self {
            integration_for_join_call: integration,
            tracking_for_join_call: tracking,
            accepted_join_call_receipt: accepted,
        }
    }

    pub(super) const fn identity(&self) -> ClassicGroupJoinIdentity {
        self.integration_for_join_call.identity()
    }

    pub(super) const fn accepted(&self) -> &AcceptedJoinGroupCall {
        &self.accepted_join_call_receipt
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        ClassicGroupJoinIntegrationOwner,
        ClassicGroupJoinTracking,
        AcceptedJoinGroupCall,
    ) {
        (
            self.integration_for_join_call,
            self.tracking_for_join_call,
            self.accepted_join_call_receipt,
        )
    }
}
