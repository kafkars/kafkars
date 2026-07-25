//! Exact prepared Sync transfer state and accepted driver-call ownership.

use kafka_client_core::{ClassicGeneration, GroupId, MemberId, MembershipCycle};

use crate::{
    clock::OperationDeadline, driver::classic_group::AcceptedSyncGroupCall,
    protocol::consumer::PreparedClassicSyncGroupRequest,
};

/// Complete immutable identity of one core-emitted follower Sync effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ClassicGroupSyncIdentity {
    group_id: GroupId,
    cycle: MembershipCycle,
    member_id: MemberId,
    generation: ClassicGeneration,
    deadline: OperationDeadline,
}

impl ClassicGroupSyncIdentity {
    pub(super) const fn new(
        group_id: GroupId,
        cycle: MembershipCycle,
        member_id: MemberId,
        generation: ClassicGeneration,
        deadline: OperationDeadline,
    ) -> Self {
        Self {
            group_id,
            cycle,
            member_id,
            generation,
            deadline,
        }
    }

    pub(super) const fn group_id(self) -> GroupId {
        self.group_id
    }

    pub(super) const fn cycle(self) -> MembershipCycle {
        self.cycle
    }

    pub(super) const fn member_id(self) -> MemberId {
        self.member_id
    }

    pub(super) const fn generation(self) -> ClassicGeneration {
        self.generation
    }

    pub(super) const fn deadline(self) -> OperationDeadline {
        self.deadline
    }
}

/// Exact core-emitted Sync intent and opaque request awaiting driver admission.
#[must_use = "a prepared classic Sync must be submitted or deliberately retained"]
pub(super) struct PreparedClassicGroupSync {
    prepared_sync_identity: ClassicGroupSyncIdentity,
    pending_sync_request: PreparedClassicSyncGroupRequest,
}

impl PreparedClassicGroupSync {
    pub(super) const fn new(
        identity: ClassicGroupSyncIdentity,
        request: PreparedClassicSyncGroupRequest,
    ) -> Self {
        Self {
            prepared_sync_identity: identity,
            pending_sync_request: request,
        }
    }

    pub(super) const fn identity(&self) -> ClassicGroupSyncIdentity {
        self.prepared_sync_identity
    }

    pub(super) const fn group_id(&self) -> GroupId {
        self.prepared_sync_identity.group_id()
    }

    pub(super) const fn cycle(&self) -> MembershipCycle {
        self.prepared_sync_identity.cycle()
    }

    pub(super) const fn member_id(&self) -> MemberId {
        self.prepared_sync_identity.member_id()
    }

    pub(super) const fn generation(&self) -> ClassicGeneration {
        self.prepared_sync_identity.generation()
    }

    pub(super) const fn deadline(&self) -> OperationDeadline {
        self.prepared_sync_identity.deadline()
    }

    pub(super) fn into_parts(self) -> (ClassicGroupSyncIdentity, PreparedClassicSyncGroupRequest) {
        (self.prepared_sync_identity, self.pending_sync_request)
    }
}

/// Linear pairing of one Sync identity and its exact accepted-call receipt.
#[must_use = "a driver-owned classic Sync must settle or recover after shutdown"]
pub(super) struct ClassicGroupSyncDriverOwner {
    driver_sync_identity: ClassicGroupSyncIdentity,
    accepted_sync_receipt: AcceptedSyncGroupCall,
}

/// Failed Sync handoff confirmation retaining both linear inputs.
#[must_use = "a failed Sync handoff still owns the accepted driver receipt"]
pub(super) struct ClassicGroupSyncAcceptanceFailure {
    rejected_sync_identity: ClassicGroupSyncIdentity,
    unrestored_sync_receipt: AcceptedSyncGroupCall,
}

impl ClassicGroupSyncAcceptanceFailure {
    pub(super) const fn new(
        identity: ClassicGroupSyncIdentity,
        accepted: AcceptedSyncGroupCall,
    ) -> Self {
        Self {
            rejected_sync_identity: identity,
            unrestored_sync_receipt: accepted,
        }
    }

    pub(super) const fn identity(&self) -> ClassicGroupSyncIdentity {
        self.rejected_sync_identity
    }

    pub(super) fn into_parts(self) -> (ClassicGroupSyncIdentity, AcceptedSyncGroupCall) {
        (self.rejected_sync_identity, self.unrestored_sync_receipt)
    }
}

impl ClassicGroupSyncDriverOwner {
    pub(super) const fn new(
        identity: ClassicGroupSyncIdentity,
        accepted: AcceptedSyncGroupCall,
    ) -> Self {
        Self {
            driver_sync_identity: identity,
            accepted_sync_receipt: accepted,
        }
    }

    pub(super) const fn identity(&self) -> ClassicGroupSyncIdentity {
        self.driver_sync_identity
    }

    pub(super) const fn accepted(&self) -> &AcceptedSyncGroupCall {
        &self.accepted_sync_receipt
    }

    pub(super) fn into_parts(self) -> (ClassicGroupSyncIdentity, AcceptedSyncGroupCall) {
        (self.driver_sync_identity, self.accepted_sync_receipt)
    }
}
