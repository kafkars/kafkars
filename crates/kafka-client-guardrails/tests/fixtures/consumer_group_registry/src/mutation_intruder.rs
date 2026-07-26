//! Foreign group-registry mutation forbidden by this fixture.

struct GroupOffsetCommitHost;
struct GroupSessionCatalog;
struct ClassicGroupEntryFault;
struct ClassicHeartbeatShutdownRecovery;
struct ClassicCoordinatorInvalidations;
struct ClassicCoordinatorInvalidationShutdownRecovery;
struct RecoveredClassicHeartbeatOwnership;
struct JoinGroupShutdownRecovery;
struct RecoveredJoinGroupOwnership;
struct RecoveredSyncGroupOwnership;
struct SyncGroupShutdownRecovery;
struct TrackedClassicHeartbeatCalls;
struct TrackedJoinGroupCalls;
struct TrackedSyncGroupCalls;

struct GroupConsumerRegistry {
    entries: Vec<u64>,
    next_group_id: Option<u64>,
    retained_group_bytes: usize,
    accepting: bool,
    join_calls: Option<TrackedJoinGroupCalls>,
    sync_calls: Option<TrackedSyncGroupCalls>,
    heartbeat_calls: Option<TrackedClassicHeartbeatCalls>,
    coordinator_invalidations: Option<ClassicCoordinatorInvalidations>,
    join_shutdown_recovery: Option<JoinGroupShutdownRecovery>,
    sync_shutdown_recovery: Option<SyncGroupShutdownRecovery>,
    heartbeat_shutdown_recovery: Option<ClassicHeartbeatShutdownRecovery>,
    coordinator_invalidation_shutdown_recovery:
        Option<ClassicCoordinatorInvalidationShutdownRecovery>,
    join_recovery_fault: Option<RecoveredJoinGroupOwnership>,
    sync_recovery_fault: Option<RecoveredSyncGroupOwnership>,
    heartbeat_recovery_fault: Option<RecoveredClassicHeartbeatOwnership>,
    offset_commits: GroupOffsetCommitHost,
}

struct GroupConsumerEntry {
    state: u8,
    catalog: GroupSessionCatalog,
    fault: Option<ClassicGroupEntryFault>,
}

fn mutate_registry(owner: &mut GroupConsumerRegistry) {
    owner.entries.clear();
    owner.next_group_id = None;
    owner.retained_group_bytes = 0;
    owner.accepting = false;
    let _joins = owner.join_calls.take();
    let _syncs = owner.sync_calls.take();
    let _heartbeats = owner.heartbeat_calls.take();
    let _invalidations = owner.coordinator_invalidations.take();
    let _join_recovery = owner.join_shutdown_recovery.take();
    let _sync_recovery = owner.sync_shutdown_recovery.take();
    let _heartbeat_recovery = owner.heartbeat_shutdown_recovery.take();
    let _invalidation_recovery = owner.coordinator_invalidation_shutdown_recovery.take();
    let _join_fault = owner.join_recovery_fault.take();
    let _sync_fault = owner.sync_recovery_fault.take();
    let _heartbeat_fault = owner.heartbeat_recovery_fault.take();
    let _borrow = &mut owner.offset_commits;
}

fn mutate_entry(owner: &mut GroupConsumerEntry) {
    owner.state = 1;
    let _borrow = &mut owner.catalog;
    owner.fault = None;
}
