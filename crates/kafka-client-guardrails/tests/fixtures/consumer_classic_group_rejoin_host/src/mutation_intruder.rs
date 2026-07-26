//! Deliberate foreign mutation of one hosted rejoin schedule state.

struct ClassicGroupRejoinExecution {
    rejoin_execution_state: usize,
}

struct ClassicCoordinatorRediscovery {
    rediscovery_state: usize,
}

fn steal(owner: &mut ClassicGroupRejoinExecution) {
    owner.rejoin_execution_state = 1;
}

fn steal_rediscovery(owner: &mut ClassicCoordinatorRediscovery) {
    owner.rediscovery_state = 1;
}
