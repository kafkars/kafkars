//! Foreign group-session catalog mutation forbidden by this fixture.

struct GroupSessionCatalog {
    next_member_id: u64,
    next_topic_id: u64,
    retained_topic_name_bytes: usize,
    topics_by_name: Vec<u64>,
    topics_by_id: Vec<u64>,
    current: Option<u64>,
}

struct ClassicGroupOwner {
    machine: u64,
    pending: Option<u64>,
}

impl GroupSessionCatalog {
    fn replace(&mut self) {
        self.next_member_id = 2;
        self.next_topic_id = 2;
        self.retained_topic_name_bytes = 12;
        self.topics_by_name.clear();
        self.topics_by_id.clear();
        self.current = Some(1);
    }
}

impl ClassicGroupOwner {
    fn intrude(&mut self) {
        self.machine = 2;
        self.pending = Some(1);
    }
}
