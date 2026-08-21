//! Public `StreamsGroup` description result tests.

use std::time::Duration;

use super::{DescribeStreamsGroupResult, StreamsGroupDescription};

#[test]
fn result_preserves_throttle_and_typed_description() {
    let description = StreamsGroupDescription::new(
        "streams-workers".to_owned(),
        "Empty".to_owned(),
        1,
        2,
        None,
        Vec::new(),
        None,
        None,
        None,
    );
    let result = DescribeStreamsGroupResult::new(Duration::from_millis(17), description);

    assert_eq!(result.throttle_time(), Duration::from_millis(17));
    assert_eq!(result.description().group_id(), "streams-workers");
    assert_eq!(result.into_description().authorized_operations(), None);
}
