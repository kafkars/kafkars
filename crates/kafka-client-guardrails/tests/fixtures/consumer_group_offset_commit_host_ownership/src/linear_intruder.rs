//! Deliberately cloneable group offset-commit host owners.

#[derive(Clone, Copy)]
struct AcceptedGroupOffsetCommit;

#[derive(Clone, Copy)]
struct GroupOffsetCommitOperation;

#[derive(Clone, Copy)]
struct GroupOffsetCommitSubmission;

#[derive(Clone, Copy)]
struct GroupOffsetCommitAttempt;

#[derive(Clone, Copy)]
struct GroupOffsetCommitPreparationFault;

#[derive(Clone, Copy)]
struct GroupOffsetCommitSettlementFault;

#[derive(Clone, Copy)]
struct GroupOffsetCommitHost;
