//! Declarative boundary for concrete classic Join and Sync call ownership.
#![expect(
    unused_imports,
    reason = "classic membership executor consumes this closed raw-call API next"
)]

mod join_group_calls;
#[cfg(test)]
mod join_group_calls_test;
mod join_group_reconciliation;
#[cfg(test)]
mod join_group_reconciliation_test;
mod join_group_settlement;
mod join_group_settlement_owner;
#[cfg(test)]
mod join_group_settlement_owner_test;
#[cfg(test)]
mod join_group_settlement_test;
mod join_group_terminal;
#[cfg(test)]
mod join_group_terminal_test;
mod sync_group_calls;
#[cfg(test)]
mod sync_group_calls_test;
mod sync_group_reconciliation;
#[cfg(test)]
mod sync_group_reconciliation_test;
mod sync_group_settlement;
mod sync_group_settlement_owner;
#[cfg(test)]
mod sync_group_settlement_owner_test;
#[cfg(test)]
mod sync_group_settlement_test;
mod sync_group_terminal;
#[cfg(test)]
mod sync_group_terminal_test;

pub(crate) use join_group_calls::{
    AcceptedJoinGroupCall, JoinGroupCallPermit, JoinGroupCallReservationError,
    TrackedJoinGroupCalls,
};
pub(crate) use join_group_reconciliation::{
    JoinGroupShutdownReconciliationError, JoinGroupShutdownReconciliationFailure,
    RecoveredJoinGroupOwnership,
};
pub(crate) use join_group_settlement::{
    JoinGroupBeginError, JoinGroupConfirmationError, JoinGroupConfirmationFailure, JoinGroupPoll,
    JoinGroupRestoreError, JoinGroupRestoreFailure, RecoveredJoinGroupConfirmation,
};
pub(crate) use join_group_settlement_owner::JoinGroupShutdownRecovery;
pub(crate) use join_group_terminal::{
    JoinGroupAdmissionFailure, JoinGroupCallKey, JoinGroupCompletionFailure,
    JoinGroupCompletionObservation, JoinGroupTerminal, RecoveredJoinGroupCall,
};
pub(crate) use sync_group_calls::{
    AcceptedSyncGroupCall, SyncGroupCallPermit, SyncGroupCallReservationError,
    TrackedSyncGroupCalls,
};
pub(crate) use sync_group_reconciliation::{
    RecoveredSyncGroupOwnership, SyncGroupShutdownReconciliationError,
    SyncGroupShutdownReconciliationFailure,
};
pub(crate) use sync_group_settlement::{
    RecoveredSyncGroupConfirmation, SyncGroupBeginError, SyncGroupConfirmationError,
    SyncGroupConfirmationFailure, SyncGroupPoll, SyncGroupRestoreError, SyncGroupRestoreFailure,
};
pub(crate) use sync_group_settlement_owner::SyncGroupShutdownRecovery;
pub(crate) use sync_group_terminal::{
    RecoveredSyncGroupCall, SyncGroupAdmissionFailure, SyncGroupCallKey,
    SyncGroupCompletionFailure, SyncGroupCompletionObservation, SyncGroupTerminal,
};
