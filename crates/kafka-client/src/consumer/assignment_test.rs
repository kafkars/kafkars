//! Public direct-assignment scalar-value contract.

use super::{StartPosition, TopicPartition};

#[test]
fn topic_partition_requires_explicit_start_without_inventing_policy() {
    let partition = TopicPartition::new("audit-log", 7);
    assert_eq!(partition.topic(), "audit-log");
    assert_eq!(partition.partition(), 7);
    assert_eq!(partition.start_position(), None);

    let partition = partition.start_at(StartPosition::Offset(11));
    assert_eq!(partition.start_position(), Some(StartPosition::Offset(11)));
}
