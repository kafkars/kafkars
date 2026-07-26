//! Deliberately cloneable leader partition-count ownership.

#[derive(Clone, Copy)]
struct PreparedClassicGroupPartitionCounts {
    partition_count_cycle: usize,
    partition_count_topics: &'static [usize],
    partition_count_deadline: usize,
}

#[derive(Clone, Copy)]
struct ClassicGroupPartitionCountCall {
    partition_count_identity: usize,
    partition_count_topic: usize,
    partition_count_driver_call: usize,
}
