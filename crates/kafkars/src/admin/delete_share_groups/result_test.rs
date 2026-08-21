//! Result projection checks for share-group deletion.

use std::time::Duration;

use super::DeleteShareGroupsResult;
use crate::admin::{BatchResult, DeleteConsumerGroupsResult};

#[test]
fn result_preserves_throttle_and_caller_order() {
    let inner = DeleteConsumerGroupsResult::new(
        Duration::from_millis(17),
        BatchResult::new(vec![
            ("share-b".to_owned(), Ok(())),
            ("share-a".to_owned(), Ok(())),
        ]),
    );
    let result = DeleteShareGroupsResult::from_consumer(inner);

    assert_eq!(result.throttle_time(), Duration::from_millis(17));
    assert_eq!(result.groups().entries()[0].0, "share-b");
    assert_eq!(result.into_groups().entries()[1].0, "share-a");
}
