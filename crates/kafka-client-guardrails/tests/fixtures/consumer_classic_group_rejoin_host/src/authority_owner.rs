//! Valid declaration sites for classic rejoin authority fields.

struct ClassicGroupRejoinExecution {
    rejoin_execution_state: usize,
}

struct PendingClassicRejoinJoin {
    pending_rejoin_group_id: usize,
    pending_rejoin_cycle: usize,
    pending_rejoin_protocol: usize,
    pending_rejoin_timing: usize,
    pending_rejoin_deadline: usize,
}

struct ClassicRejoinPostCore {
    post_core_rejoin_join: usize,
    post_core_rejoin_other: usize,
    post_core_rejoin_failure: usize,
}

struct ClassicRejectionPostCore {
    post_core_rejection_effects: usize,
    post_core_rejection_failure: usize,
}

struct ClassicCoordinatorRediscovery {
    rediscovery_state: usize,
}
