//! Deliberate foreign mutation of the candidate owner.

use crate::owner::{ClassicGroupOwner, PreparedClassicGroupPartitionCounts};

fn replace(owner: &mut ClassicGroupOwner) {
    owner.pending = Some(7);
}

fn replace_progress(prepared: &mut PreparedClassicGroupPartitionCounts) {
    prepared.partition_count_values.push(8);
    prepared.partition_count_metadata_generation = Some(9);
}
