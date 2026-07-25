//! Forbidden constructor of classic-group recovered ownership.

fn forge_join_active<T>(value: T) {
    RecoveredJoinGroupOwnership::seal_recovered_join_group_active(value);
}

fn forge_join_settled<T>(value: T) {
    RecoveredJoinGroupOwnership::seal_recovered_join_group_settled(value);
}

fn forge_join_pending<T>(value: T) {
    RecoveredJoinGroupOwnership::seal_recovered_join_group_pending(value);
}

fn forge_join_completion<T>(value: T) {
    RecoveredJoinGroupOwnership::seal_recovered_join_group_completion(value);
}

fn forge_sync_active<T>(value: T) {
    RecoveredSyncGroupOwnership::seal_recovered_sync_group_active(value);
}

fn forge_sync_settled<T>(value: T) {
    RecoveredSyncGroupOwnership::seal_recovered_sync_group_settled(value);
}

fn forge_sync_pending<T>(value: T) {
    RecoveredSyncGroupOwnership::seal_recovered_sync_group_pending(value);
}

fn forge_sync_completion<T>(value: T) {
    RecoveredSyncGroupOwnership::seal_recovered_sync_group_completion(value);
}
