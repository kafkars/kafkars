//! Deliberate foreign construction of heartbeat identities and state.

fn steal<A, D, P>(attempt: A, deadline: D, policy: P) {
    ClassicHeartbeatAttempt::first(attempt, attempt);
    let _ = (deadline, policy);
}
