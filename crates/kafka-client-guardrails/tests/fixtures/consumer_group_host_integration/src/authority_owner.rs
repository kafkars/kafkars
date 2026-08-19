//! Valid declaration sites for classic membership host authorities.

struct PreparedClassicGroupJoin {
    prepared_join_identity: usize,
}

struct ClassicGroupJoinHandoff {
    handed_off_join: usize,
}

struct ClassicGroupJoinDriverAcceptance {
    accepted_join: usize,
}

struct ClassicGroupJoinTracking {
    tracked_join_identity: usize,
}

struct ClassicGroupJoinIntegrationOwner {
    driver_owned_join: usize,
}

struct ClassicGroupExecution {
    classic_execution_state: usize,
}

struct GroupConsumerShardState {
    registry_owner: usize,
    admission_fence: usize,
    reactor_wake: usize,
    group_recv_signal: usize,
    group_recv_publisher: usize,
    port_contention_handoff: usize,
}
