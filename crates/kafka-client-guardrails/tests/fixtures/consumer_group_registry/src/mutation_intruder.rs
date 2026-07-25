//! Foreign group-registry mutation forbidden by this fixture.

struct GroupOffsetCommitHost;
struct GroupSessionCatalog;

struct GroupConsumerRegistry {
    entries: Vec<u64>,
    next_group_id: Option<u64>,
    retained_group_bytes: usize,
    accepting: bool,
    offset_commits: GroupOffsetCommitHost,
}

struct GroupConsumerEntry {
    state: u8,
    catalog: GroupSessionCatalog,
}

fn mutate_registry(owner: &mut GroupConsumerRegistry) {
    owner.entries.clear();
    owner.next_group_id = None;
    owner.retained_group_bytes = 0;
    owner.accepting = false;
    let _borrow = &mut owner.offset_commits;
}

fn mutate_entry(owner: &mut GroupConsumerEntry) {
    owner.state = 1;
    let _borrow = &mut owner.catalog;
}
