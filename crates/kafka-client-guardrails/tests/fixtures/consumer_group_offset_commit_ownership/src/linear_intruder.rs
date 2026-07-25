//! Forbidden cloneable group offset commit owners.

#[derive(Clone, Copy)]
struct ClassicGroupCommitSession;

#[derive(Clone, Copy)]
struct PreparedGroupOffsetCommit;

#[derive(Clone, Copy)]
struct PreparedGroupOffsetCommitRequest;

#[derive(Clone, Copy)]
struct GroupOffsetCommitEntryReservation;

#[derive(Clone, Copy)]
struct GroupOffsetCommitResultReservation;

#[derive(Clone, Copy)]
struct GroupOffsetCommitPreparationError;

#[derive(Clone, Copy)]
struct GroupOffsetCommitCallPermit;

#[derive(Clone, Copy)]
struct TrackedGroupOffsetCommitCall;

#[derive(Clone, Copy)]
struct GroupOffsetCommitPrebuiltAdmissionFailure;

#[derive(Clone, Copy)]
struct GroupOffsetCommitCompletionFailure;

#[derive(Clone, Copy)]
struct GroupOffsetCommitCompletionRecovery;

#[derive(Clone, Copy)]
struct RecoveredGroupOffsetCommitSettlement;

#[derive(Clone, Copy)]
struct GroupOffsetCommitShutdownRecovery;

#[derive(Clone, Copy)]
struct SettledGroupOffsetCommitCall;

#[derive(Clone, Copy)]
struct PendingGroupOffsetCommitConfirmation;

#[derive(Clone, Copy)]
struct GroupOffsetCommitRestoreFailure;

#[derive(Clone, Copy)]
struct TrackedGroupOffsetCommitCalls;
