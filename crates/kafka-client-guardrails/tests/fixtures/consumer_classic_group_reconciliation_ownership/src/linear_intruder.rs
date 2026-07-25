//! Deliberately cloneable classic-group shutdown reconciliation owners.

#[derive(Clone, Copy)]
struct RecoveredJoinGroupOwnership;
#[derive(Clone, Copy)]
struct JoinGroupShutdownReconciliationFailure;
#[derive(Clone, Copy)]
struct RecoveredSyncGroupOwnership;
#[derive(Clone, Copy)]
struct SyncGroupShutdownReconciliationFailure;
