//! Foreign fixed-assignment state mutation forbidden by the fixture.

struct AssignedTopics {
    next_topic_id: u64,
    retained_name_bytes: usize,
    by_name: Vec<u64>,
    by_id: Vec<u64>,
    partitions: Vec<u64>,
}

impl AssignedTopics {
    fn replace(&mut self) {
        self.next_topic_id = 2;
        self.retained_name_bytes = 12;
        self.by_name.clear();
        self.by_id.clear();
        self.partitions.clear();
    }
}
