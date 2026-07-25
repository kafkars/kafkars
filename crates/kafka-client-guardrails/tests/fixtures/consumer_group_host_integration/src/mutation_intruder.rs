//! Foreign mutation of classic membership execution and shard state.

struct ClassicGroupExecution {
    classic_execution_state: usize,
}

struct GroupConsumerShardState {
    registry_owner: usize,
    admission_fence: bool,
}

fn mutate_execution(owner: &mut ClassicGroupExecution) {
    owner.classic_execution_state = 2;
}

fn mutate_shard(owner: &mut GroupConsumerShardState) {
    owner.registry_owner = 2;
    owner.admission_fence = true;
}
