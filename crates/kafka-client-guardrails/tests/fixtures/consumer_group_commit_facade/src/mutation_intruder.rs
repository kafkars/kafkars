//! Deliberately mutates commit observation fields outside their owners.

struct CommitConsumerCheckpoint {
    inner: usize,
}

struct GroupConsumerCommit {
    inner: usize,
}

fn violate(public: &mut CommitConsumerCheckpoint, bridge: &mut GroupConsumerCommit) {
    public.inner += 1;
    bridge.inner += 1;
}
