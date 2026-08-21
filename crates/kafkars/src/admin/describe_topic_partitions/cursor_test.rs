//! Explicit page-cursor scalar preservation tests.

use super::DescribeTopicPartitionsCursor;

#[test]
fn cursor_is_inert_and_preserves_even_invalid_intent_for_submit_validation() {
    let cursor = DescribeTopicPartitionsCursor::new("", -1);

    assert_eq!(cursor.topic_name(), "");
    assert_eq!(cursor.partition_index(), -1);
    assert_eq!(cursor.into_parts(), (String::new(), -1));
}
