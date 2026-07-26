//! Valid sole declaration and mutation owners for the fixture.

struct PreparedClassicGroupPartitionCounts {
    partition_count_cycle: usize,
    partition_count_topics: Vec<usize>,
    partition_count_values: Vec<usize>,
    partition_count_metadata_generation: Option<usize>,
    partition_count_deadline: usize,
}

struct ClassicGroupOwner {
    pending: Option<usize>,
}

struct ClassicGroupPartitionCountCall {
    partition_count_identity: usize,
    partition_count_topic: usize,
    partition_count_driver_call: usize,
}

fn clear(owner: &mut ClassicGroupOwner) {
    owner.pending = None;
}
