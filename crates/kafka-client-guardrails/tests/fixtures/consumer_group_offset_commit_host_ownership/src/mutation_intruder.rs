//! Deliberately mutates group offset-commit host state outside its owners.

struct GroupOffsetCommitHost {
    operations: Vec<u8>,
    retained_bytes: usize,
    accepting: bool,
    fault: usize,
    preparation_fault: usize,
    settlement_fault: usize,
    shutdown_recovery: usize,
    effect_fault: usize,
    recovery_faults: Vec<u8>,
    published_bytes: Vec<u8>,
    reclaim_pending: usize,
    next_operation_id: usize,
}

struct GroupOffsetCommitOperation {
    attempt: usize,
    terminal: usize,
}

fn steal_host(host: &mut GroupOffsetCommitHost) {
    host.operations.push(1);
    host.retained_bytes += 1;
    host.accepting = false;
    host.fault = 1;
    host.preparation_fault = 1;
    host.settlement_fault = 1;
    host.shutdown_recovery = 1;
    host.effect_fault = 1;
    host.recovery_faults.push(1);
    host.published_bytes.push(1);
    host.reclaim_pending = 1;
    host.next_operation_id = 1;
}

fn steal_operation(operation: &mut GroupOffsetCommitOperation) {
    operation.attempt = 1;
    operation.terminal = 1;
}
