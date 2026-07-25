//! Valid declaration site for follower call and Sync authorities.

struct ClassicGroupJoinCallOwner {
    integration_for_join_call: usize,
    tracking_for_join_call: usize,
    accepted_join_call_receipt: usize,
}

struct ClassicGroupJoinAcceptanceFailure {
    rejected_join_acceptance: usize,
    unrestored_join_receipt: usize,
}

struct PreparedClassicGroupSync {
    prepared_sync_identity: usize,
    pending_sync_request: usize,
}

struct ClassicGroupSyncDriverOwner {
    driver_sync_identity: usize,
    accepted_sync_receipt: usize,
}

struct ClassicGroupSyncAcceptanceFailure {
    rejected_sync_identity: usize,
    unrestored_sync_receipt: usize,
}

struct SyncInterpretationFailure {
    sync_failure_kind: usize,
    restorable_sync_terminal: usize,
}
