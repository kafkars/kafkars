//! Public `ShareGroup` description result tests.

use std::time::Duration;

use super::{DescribeShareGroupResult, ShareGroupDescription};

#[test]
fn result_preserves_throttle_and_typed_description() {
    let description = ShareGroupDescription::new(
        "share-workers".to_owned(),
        "Empty".to_owned(),
        1,
        2,
        "uniform".to_owned(),
        Vec::new(),
        None,
    );
    let result = DescribeShareGroupResult::new(Duration::from_millis(17), description);

    assert_eq!(result.throttle_time(), Duration::from_millis(17));
    assert_eq!(result.description().group_id(), "share-workers");
    assert_eq!(result.into_description().authorized_operations(), None);
}
