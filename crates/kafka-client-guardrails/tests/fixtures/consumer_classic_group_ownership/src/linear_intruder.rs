//! Forbidden cloneable classic-group owners.

#[derive(Clone, Copy)]
struct ClassicSubscription;

#[derive(Clone, Copy)]
struct ClassicJoinMember;

#[derive(Clone, Copy)]
struct ClassicJoinMembers;

#[derive(Clone, Copy)]
struct ClassicAssignmentPlan;

#[derive(Clone, Copy)]
struct ClassicMemberAssignment;

#[derive(Clone, Copy)]
enum ClassicGroupInput {
    Close,
}

#[derive(Clone, Copy)]
enum ClassicGroupEffect {
    Join,
}

#[derive(Clone, Copy)]
struct ClassicGroupTransition;

#[derive(Clone, Copy)]
struct ClassicGroupMachine;

#[derive(Clone, Copy)]
struct ClassicHeartbeatState;

#[derive(Clone, Copy)]
struct ClassicProcessingLease;

#[derive(Clone, Copy)]
struct PreparedClassicProcessingLeaseActivation;

#[derive(Clone, Copy)]
struct PreparedClassicProcessingLeaseRevocation;

#[derive(Clone, Copy)]
struct ClassicGracefulRevocation;
