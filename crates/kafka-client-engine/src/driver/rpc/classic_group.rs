//! Declarative boundary for concrete classic Join and Sync call ownership.
#![expect(
    unused_imports,
    reason = "classic membership executor consumes this closed raw-call API next"
)]

mod heartbeat_calls;
#[cfg(test)]
mod heartbeat_calls_test;
mod heartbeat_reconciliation;
#[cfg(test)]
mod heartbeat_reconciliation_test;
mod heartbeat_settlement;
mod heartbeat_settlement_owner;
#[cfg(test)]
mod heartbeat_settlement_owner_test;
#[cfg(test)]
mod heartbeat_settlement_test;
mod heartbeat_terminal;
#[cfg(test)]
mod heartbeat_terminal_test;
#[cfg(test)]
mod heartbeat_test_fixture;
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
#[cfg(test)]
mod terminal_test_fixture;

pub(crate) use heartbeat_calls::{
    AcceptedClassicHeartbeatCall, ClassicHeartbeatCallPermit, ClassicHeartbeatCallReservationError,
    TrackedClassicHeartbeatCalls,
};
pub(crate) use heartbeat_reconciliation::{
    ClassicHeartbeatShutdownReconciliationError, ClassicHeartbeatShutdownReconciliationFailure,
    RecoveredClassicHeartbeatOwnership,
};
pub(crate) use heartbeat_settlement::{
    ClassicHeartbeatBeginError, ClassicHeartbeatConfirmationError,
    ClassicHeartbeatConfirmationFailure, ClassicHeartbeatPoll, ClassicHeartbeatRestoreError,
    ClassicHeartbeatRestoreFailure, RecoveredClassicHeartbeatConfirmation,
};
pub(crate) use heartbeat_settlement_owner::ClassicHeartbeatShutdownRecovery;
pub(crate) use heartbeat_terminal::{
    ClassicHeartbeatAdmissionFailure, ClassicHeartbeatCallKey, ClassicHeartbeatCompletionFailure,
    ClassicHeartbeatCompletionObservation, ClassicHeartbeatTerminal, RecoveredClassicHeartbeatCall,
};
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
#[cfg(test)]
pub(crate) use terminal_test_fixture::{
    install_follower_join_terminal, install_leader_join_terminal, install_malformed_sync_terminal,
    install_sync_assignment_terminal,
};
