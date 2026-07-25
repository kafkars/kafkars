//! Deliberate foreign construction of recovered classic Heartbeat ownership.

fn steal<T>(owner: T) {
    RecoveredClassicHeartbeatOwnership::seal_active(owner);
    RecoveredClassicHeartbeatOwnership::seal_settled(owner);
    RecoveredClassicHeartbeatOwnership::seal_pending(owner);
    RecoveredClassicHeartbeatOwnership::seal_completion(owner);
}
