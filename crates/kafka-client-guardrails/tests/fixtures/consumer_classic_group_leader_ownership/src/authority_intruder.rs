//! Deliberate foreign construction of the prepared partition-count authority.

use crate::owner::{ClassicGroupPartitionCountCall, PreparedClassicGroupPartitionCounts};

fn forge() -> PreparedClassicGroupPartitionCounts {
    PreparedClassicGroupPartitionCounts {
        partition_count_cycle: 1,
        partition_count_topics: vec![2],
        partition_count_values: vec![3],
        partition_count_metadata_generation: Some(4),
        partition_count_deadline: 5,
    }
}

fn forge_call() -> ClassicGroupPartitionCountCall {
    ClassicGroupPartitionCountCall {
        partition_count_identity: 6,
        partition_count_topic: 7,
        partition_count_driver_call: 8,
    }
}
