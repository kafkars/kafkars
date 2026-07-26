//! Exact prepared Join transfer state between local scheduling and driver ownership.

use kafka_client_core::{
    ClassicGeneration, ClassicGroupTiming, ClassicProtocol, GroupId, LiveGroupAssignment,
    MembershipCycle,
};

use super::classic_group_assignment::ClassicGroupAssignmentPreparationFailureKind;
use super::{
    classic_group_join_call::ClassicGroupJoinCallOwner,
    classic_group_partition_count_call::{
        ClassicGroupPartitionCountCall, ClassicGroupPartitionCountCallIdentity,
    },
    classic_group_partition_counts::PreparedClassicGroupPartitionCounts,
    classic_group_sync::{
        ClassicGroupSyncDriverOwner, ClassicGroupSyncIdentity, PreparedClassicGroupSync,
    },
};

use crate::clock::OperationDeadline;

/// Complete immutable identity of one core-emitted Join effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ClassicGroupJoinIdentity {
    group_identity: GroupId,
    join_cycle: MembershipCycle,
    selected_protocol: ClassicProtocol,
    group_timing: ClassicGroupTiming,
    absolute_deadline: OperationDeadline,
}

/// Exact core-emitted Join intent awaiting concrete protocol execution.
#[must_use = "a prepared classic Join must remain retained until execution consumes it"]
pub(super) struct PreparedClassicGroupJoin {
    prepared_join_identity: ClassicGroupJoinIdentity,
}

/// Linear Join owner while a concrete driver submission is attempted.
#[must_use = "a Join handoff must be restored or converted from a successful driver acceptance"]
pub(super) struct ClassicGroupJoinHandoff {
    handed_off_join: PreparedClassicGroupJoin,
}

/// Linear proof that the integration seam accepted driver ownership.
#[must_use = "driver acceptance must be confirmed by the membership execution owner"]
pub(super) struct ClassicGroupJoinDriverAcceptance {
    accepted_join: PreparedClassicGroupJoin,
}

/// Linear tracked-call identity retained outside local scheduling.
#[must_use = "tracked Join ownership must settle or return through shutdown recovery"]
pub(super) struct ClassicGroupJoinTracking {
    tracked_join_identity: ClassicGroupJoinIdentity,
}

/// Exact recovery owner retained while local deadline scheduling is disarmed.
#[must_use = "driver-owned Join must settle or recover its exact prepared owner"]
pub(super) struct ClassicGroupJoinIntegrationOwner {
    driver_owned_join: PreparedClassicGroupJoin,
}

pub(super) enum ClassicGroupExecutionState {
    Idle,
    PreparedJoin(PreparedClassicGroupJoin),
    JoinHandoff(ClassicGroupJoinIdentity),
    JoinDriverOwned(ClassicGroupJoinCallOwner),
    JoinConfirmationPending {
        call: ClassicGroupJoinCallOwner,
        successor: ClassicGroupJoinSuccessor,
    },
    PreparedPartitionCounts(PreparedClassicGroupPartitionCounts),
    PartitionCountHandoff {
        prepared: PreparedClassicGroupPartitionCounts,
        identity: ClassicGroupPartitionCountCallIdentity,
    },
    PartitionCountDriverOwned {
        prepared: PreparedClassicGroupPartitionCounts,
        call: ClassicGroupPartitionCountCall,
    },
    PartitionCountCompletionFault {
        prepared: PreparedClassicGroupPartitionCounts,
        call: ClassicGroupPartitionCountCall,
    },
    PartitionCountsPostCore {
        _retained_partition_counts: PreparedClassicGroupPartitionCounts,
    },
    PreparedSync(PreparedClassicGroupSync),
    SyncHandoff(ClassicGroupSyncIdentity),
    SyncDriverOwned(ClassicGroupSyncDriverOwner),
    SyncConfirmationPending(ClassicGroupSyncDriverOwner),
    CloseFault {
        revoke_assignment: LiveGroupAssignment,
        revoke_generation: ClassicGeneration,
        revoke_failure_kind: ClassicGroupAssignmentPreparationFailureKind,
    },
}

pub(super) enum ClassicGroupJoinSuccessor {
    Idle,
    PartitionCounts(PreparedClassicGroupPartitionCounts),
    Sync(PreparedClassicGroupSync),
}

impl PreparedClassicGroupJoin {
    pub(super) const fn new(
        group_id: GroupId,
        cycle: MembershipCycle,
        protocol: ClassicProtocol,
        timing: ClassicGroupTiming,
        deadline: OperationDeadline,
    ) -> Self {
        Self {
            prepared_join_identity: ClassicGroupJoinIdentity {
                group_identity: group_id,
                join_cycle: cycle,
                selected_protocol: protocol,
                group_timing: timing,
                absolute_deadline: deadline,
            },
        }
    }

    pub(super) const fn identity(&self) -> ClassicGroupJoinIdentity {
        self.prepared_join_identity
    }

    pub(super) const fn group_id(&self) -> GroupId {
        self.prepared_join_identity.group_id()
    }

    pub(super) const fn cycle(&self) -> MembershipCycle {
        self.prepared_join_identity.cycle()
    }

    pub(super) const fn protocol(&self) -> ClassicProtocol {
        self.prepared_join_identity.protocol()
    }

    pub(super) const fn timing(&self) -> ClassicGroupTiming {
        self.prepared_join_identity.timing()
    }

    pub(super) const fn deadline(&self) -> OperationDeadline {
        self.prepared_join_identity.deadline()
    }
}

impl ClassicGroupJoinIdentity {
    pub(super) const fn group_id(self) -> GroupId {
        self.group_identity
    }

    pub(super) const fn cycle(self) -> MembershipCycle {
        self.join_cycle
    }

    pub(super) const fn protocol(self) -> ClassicProtocol {
        self.selected_protocol
    }

    pub(super) const fn timing(self) -> ClassicGroupTiming {
        self.group_timing
    }

    pub(super) const fn deadline(self) -> OperationDeadline {
        self.absolute_deadline
    }
}

impl ClassicGroupJoinHandoff {
    pub(super) const fn new(prepared: PreparedClassicGroupJoin) -> Self {
        Self {
            handed_off_join: prepared,
        }
    }

    pub(super) const fn identity(&self) -> ClassicGroupJoinIdentity {
        self.handed_off_join.identity()
    }

    pub(super) fn into_driver_acceptance(self) -> ClassicGroupJoinDriverAcceptance {
        ClassicGroupJoinDriverAcceptance {
            accepted_join: self.handed_off_join,
        }
    }

    pub(super) fn into_prepared(self) -> PreparedClassicGroupJoin {
        self.handed_off_join
    }
}

impl ClassicGroupJoinDriverAcceptance {
    pub(super) const fn identity(&self) -> ClassicGroupJoinIdentity {
        self.accepted_join.identity()
    }

    pub(super) fn into_driver_owners(
        self,
    ) -> (ClassicGroupJoinIntegrationOwner, ClassicGroupJoinTracking) {
        let identity = self.accepted_join.identity();
        (
            ClassicGroupJoinIntegrationOwner {
                driver_owned_join: self.accepted_join,
            },
            ClassicGroupJoinTracking {
                tracked_join_identity: identity,
            },
        )
    }
}

impl ClassicGroupJoinTracking {
    pub(super) const fn identity(&self) -> ClassicGroupJoinIdentity {
        self.tracked_join_identity
    }
}

impl ClassicGroupJoinIntegrationOwner {
    pub(super) const fn identity(&self) -> ClassicGroupJoinIdentity {
        self.driver_owned_join.identity()
    }

    pub(super) fn into_prepared(self) -> PreparedClassicGroupJoin {
        self.driver_owned_join
    }
}
