//! Result projection checks for streams-group deletion.

use std::time::Duration;

use super::DeleteStreamsGroupsResult;
use crate::admin::{BatchResult, DeleteConsumerGroupsResult};

#[test]
fn result_preserves_throttle_and_caller_order() {
    let inner = DeleteConsumerGroupsResult::new(
        Duration::from_millis(19),
        BatchResult::new(vec![
            ("streams-b".to_owned(), Ok(())),
            ("streams-a".to_owned(), Ok(())),
        ]),
    );
    let result = DeleteStreamsGroupsResult::from_consumer(inner);

    assert_eq!(result.throttle_time(), Duration::from_millis(19));
    assert_eq!(result.groups().entries()[0].0, "streams-b");
    assert_eq!(result.into_groups().entries()[1].0, "streams-a");
}
