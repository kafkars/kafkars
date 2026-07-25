//! Cloneable follower lifecycle owners forbidden by this fixture.

#[derive(Clone, Copy)]
struct ClassicGroupJoinCallOwner;

#[derive(Clone, Copy)]
struct ClassicGroupJoinAcceptanceFailure;

#[derive(Clone, Copy)]
enum ClassicGroupJoinSuccessor {
    Idle,
}

#[derive(Clone, Copy)]
struct PreparedClassicGroupSync;

#[derive(Clone, Copy)]
struct ClassicGroupSyncDriverOwner;

#[derive(Clone, Copy)]
struct ClassicGroupSyncAcceptanceFailure;

#[derive(Clone, Copy)]
enum ClassicGroupEntryFault {
    Frozen,
}

#[derive(Clone, Copy)]
struct SyncInterpretationFailure;

#[derive(Clone, Copy)]
enum JoinRecoveryState {
    DriverOwned,
}

#[derive(Clone, Copy)]
enum SyncRecoveryFailure {
    Semantic,
}
