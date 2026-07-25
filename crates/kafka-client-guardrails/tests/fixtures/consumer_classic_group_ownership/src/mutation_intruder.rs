//! Forbidden classic-group lifecycle mutation outside its transition owner.

struct ClassicGroupMachine {
    phase: usize,
    next_cycle: usize,
    active_cycle: usize,
    deadline: usize,
    pending_member_id: usize,
    pending_generation: usize,
    pending_members: usize,
    pending_local_slot: usize,
    pending_expected_assignment: usize,
    pending_heartbeat_liveness: usize,
    next_assignment_generation: usize,
    live_generation: usize,
    live_assignment: usize,
}

fn mutate_outside_transition(owner: &mut ClassicGroupMachine) {
    owner.phase = 1;
    owner.next_cycle = 1;
    owner.active_cycle = 1;
    owner.deadline = 1;
    owner.pending_member_id = 1;
    owner.pending_generation = 1;
    owner.pending_members = 1;
    owner.pending_local_slot = 1;
    owner.pending_expected_assignment = 1;
    owner.pending_heartbeat_liveness = 1;
    owner.next_assignment_generation = 1;
    owner.live_generation = 1;
    owner.live_assignment = 1;
}
