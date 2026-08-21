//! Typed `ShareGroup` assignment value tests.

use super::{ShareGroupAssignment, ShareGroupTopicPartitions};

#[test]
fn assignment_preserves_topic_identity_name_and_partitions() {
    let assignment = ShareGroupAssignment::new(vec![ShareGroupTopicPartitions::new(
        [7; 16],
        "orders".to_owned(),
        vec![0, 2, 7],
    )]);

    let topic = &assignment.topics()[0];
    assert_eq!(topic.topic_id(), [7; 16]);
    assert_eq!(topic.topic_name(), "orders");
    assert_eq!(topic.partitions(), [0, 2, 7]);
    assert_eq!(assignment.into_topics().len(), 1);
}
