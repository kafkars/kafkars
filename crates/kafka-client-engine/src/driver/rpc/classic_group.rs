//! Declarative boundary for concrete classic Join, Sync, and Heartbeat call ownership.

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
    AcceptedClassicHeartbeatCall, ClassicHeartbeatCallReservationError,
    TrackedClassicHeartbeatCalls,
};
pub(crate) use heartbeat_reconciliation::RecoveredClassicHeartbeatOwnership;
pub(crate) use heartbeat_settlement::{ClassicHeartbeatPoll, ClassicHeartbeatRestoreFailure};
pub(crate) use heartbeat_settlement_owner::ClassicHeartbeatShutdownRecovery;
pub(crate) use heartbeat_terminal::{
    ClassicHeartbeatAdmissionFailure, ClassicHeartbeatCallKey, ClassicHeartbeatTerminal,
};
#[cfg(test)]
pub(crate) use heartbeat_test_fixture::{
    heartbeat_attempts, install_heartbeat_broker_rejection_terminal,
    install_heartbeat_route_failure_terminal, install_heartbeat_success_terminal,
};
pub(crate) use join_group_calls::{
    AcceptedJoinGroupCall, JoinGroupCallReservationError, TrackedJoinGroupCalls,
};
pub(crate) use join_group_reconciliation::RecoveredJoinGroupOwnership;
pub(crate) use join_group_settlement::{JoinGroupPoll, JoinGroupRestoreFailure};
pub(crate) use join_group_settlement_owner::JoinGroupShutdownRecovery;
pub(crate) use join_group_terminal::{JoinGroupCallKey, JoinGroupTerminal};
pub(crate) use sync_group_calls::{
    AcceptedSyncGroupCall, SyncGroupCallReservationError, TrackedSyncGroupCalls,
};
pub(crate) use sync_group_reconciliation::RecoveredSyncGroupOwnership;
pub(crate) use sync_group_settlement::{SyncGroupPoll, SyncGroupRestoreFailure};
pub(crate) use sync_group_settlement_owner::SyncGroupShutdownRecovery;
pub(crate) use sync_group_terminal::{
    SyncGroupAdmissionFailure, SyncGroupCallKey, SyncGroupTerminal,
};
#[cfg(test)]
pub(crate) use terminal_test_fixture::{
    install_follower_join_terminal, install_join_broker_rejection_terminal,
    install_leader_join_terminal, install_malformed_sync_terminal,
    install_sync_assignment_terminal, install_sync_broker_rejection_terminal,
};
