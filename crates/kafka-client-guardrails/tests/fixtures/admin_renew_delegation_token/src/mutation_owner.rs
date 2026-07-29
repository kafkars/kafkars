//! Sole allowed API 39 state and retained-ownership mutation fixture.

struct RenewDelegationTokenMachine {
    state: usize,
}

struct RenewDelegationTokenHost {
    operations: usize,
    completions: usize,
    next_operation_id: usize,
    reclaim_pending: usize,
    retained_bytes: usize,
    accepting: usize,
    health: usize,
    published_bytes: usize,
}

struct RenewDelegationTokenOperation {
    machine: usize,
    remaining_result_bytes: usize,
    submission: usize,
    handoff: usize,
    call: usize,
    raw_terminal: usize,
    terminal: usize,
}

fn mutate(
    machine: &mut RenewDelegationTokenMachine,
    host: &mut RenewDelegationTokenHost,
    operation: &mut RenewDelegationTokenOperation,
) {
    machine.state += 1;
    host.operations += 1;
    host.completions += 1;
    host.next_operation_id += 1;
    host.reclaim_pending += 1;
    host.retained_bytes += 1;
    host.accepting += 1;
    host.health += 1;
    host.published_bytes += 1;
    operation.machine += 1;
    operation.remaining_result_bytes += 1;
    operation.submission += 1;
    operation.handoff += 1;
    operation.call += 1;
    operation.raw_terminal += 1;
    operation.terminal += 1;
}
