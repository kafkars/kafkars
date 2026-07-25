//! Bounded classic-group identity and deterministic membership ownership.

mod classic_group_assignment;
mod classic_group_assignment_decode;
mod classic_group_candidate;
mod classic_group_candidate_prepare;
mod classic_group_entry_fault;
mod classic_group_execution;
mod classic_group_execution_close;
mod classic_group_execution_handoff;
mod classic_group_execution_join_terminal;
mod classic_group_execution_recovery;
mod classic_group_execution_sync;
mod classic_group_execution_sync_terminal;
mod classic_group_join;
mod classic_group_join_call;
mod classic_group_join_execution;
mod classic_group_join_interpret;
mod classic_group_join_settlement;
mod classic_group_owner;
mod classic_group_owner_follower;
mod classic_group_recovery;
mod classic_group_sync;
mod classic_group_sync_interpret;
mod classic_group_sync_settlement;
mod classic_group_sync_submission;
mod classic_group_topics;
mod offset_commit;
mod registry;
mod registry_close;
mod registry_commit;
mod registry_cycle;
mod registry_entry;
mod registry_host;
mod registry_membership;
mod registry_port;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "awaiting private group-consumer integration")
)]
mod registry_session;
mod registry_shard;
mod registry_wake;
mod session_catalog;
mod session_catalog_assignment;

#[cfg(test)]
mod classic_group_assignment_bounds_test;
#[cfg(test)]
mod classic_group_assignment_decode_subscription_test;
#[cfg(test)]
mod classic_group_assignment_decode_test;
#[cfg(test)]
mod classic_group_assignment_test;
#[cfg(test)]
mod classic_group_candidate_prepare_test;
#[cfg(test)]
mod classic_group_candidate_test;
#[cfg(test)]
mod classic_group_entry_fault_test;
#[cfg(test)]
mod classic_group_execution_close_test;
#[cfg(test)]
mod classic_group_execution_handoff_test;
#[cfg(test)]
mod classic_group_execution_join_terminal_test;
#[cfg(test)]
mod classic_group_execution_recovery_test;
#[cfg(test)]
mod classic_group_execution_sync_terminal_test;
#[cfg(test)]
mod classic_group_execution_sync_test;
#[cfg(test)]
mod classic_group_execution_test;
#[cfg(test)]
mod classic_group_join_call_test;
#[cfg(test)]
mod classic_group_join_execution_test;
#[cfg(test)]
mod classic_group_join_interpret_test;
#[cfg(test)]
mod classic_group_join_settlement_test;
#[cfg(test)]
mod classic_group_join_test;
#[cfg(test)]
mod classic_group_owner_follower_test;
#[cfg(test)]
mod classic_group_owner_test;
#[cfg(test)]
mod classic_group_recovery_test;
#[cfg(test)]
mod classic_group_sync_interpret_test;
#[cfg(test)]
mod classic_group_sync_settlement_test;
#[cfg(test)]
mod classic_group_sync_submission_test;
#[cfg(test)]
mod classic_group_sync_test;
#[cfg(test)]
mod classic_group_test_support;
#[cfg(test)]
mod classic_group_topics_test;
#[cfg(test)]
mod registry_close_test;
#[cfg(test)]
mod registry_commit_test;
#[cfg(test)]
mod registry_cycle_test;
#[cfg(test)]
mod registry_entry_test;
#[cfg(test)]
mod registry_host_test;
#[cfg(test)]
mod registry_membership_test;
#[cfg(test)]
mod registry_port_test;
#[cfg(test)]
mod registry_session_test;
#[cfg(test)]
mod registry_shard_test;
#[cfg(test)]
mod registry_test;
#[cfg(test)]
mod registry_test_support;
#[cfg(test)]
mod registry_wake_test;
#[cfg(test)]
mod session_catalog_assignment_test;
#[cfg(test)]
mod session_catalog_identity_test;
#[cfg(test)]
mod session_catalog_test;

pub(crate) use registry::GroupConsumerRegistry;
pub(crate) use registry_host::GroupConsumerHostError;
pub(crate) use registry_port::GroupConsumerPort;
pub(crate) use registry_shard::{GroupConsumerShardLockError, GroupConsumerShardOwner};
pub(crate) use registry_wake::{GroupConsumerShardWake, GroupConsumerShardWakeError};
