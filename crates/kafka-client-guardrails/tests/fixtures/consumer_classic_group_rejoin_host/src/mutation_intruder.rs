//! Deliberate foreign mutation of one hosted rejoin schedule state.

struct ClassicGroupRejoinExecution {
    rejoin_execution_state: usize,
}

fn steal(owner: &mut ClassicGroupRejoinExecution) {
    owner.rejoin_execution_state = 1;
}
