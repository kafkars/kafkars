//! Deliberately cloneable classic-group protocol ownership values.

#[derive(Clone, Copy)]
struct ClassicJoinedMember;
#[derive(Clone, Copy)]
struct ClassicJoinedRole;
#[derive(Clone, Copy)]
struct ClassicJoinedGroup;
#[derive(Clone, Copy)]
struct ClassicJoinOutcome;
#[derive(Clone, Copy)]
struct ClassicSyncMember;
#[derive(Clone, Copy)]
struct ClassicSyncTopic;
#[derive(Clone, Copy)]
struct NamedAssignmentPartition;
#[derive(Clone, Copy)]
struct ClassicSyncOutcome;
#[derive(Clone, Copy)]
struct PreparedClassicJoinGroupRequest;
#[derive(Clone, Copy)]
struct PreparedClassicSyncGroupRequest;
