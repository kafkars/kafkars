//! `ShareGroup` offset-deletion result ordering and throttle tests.

use std::time::Duration;

use crate::{BatchResult, ErrorKind, KafkaError};

use super::DeleteShareGroupOffsetsResult;

#[test]
fn result_preserves_throttle_topic_ids_and_engine_supplied_caller_order() {
    let topics = BatchResult::new(vec![
        ("orders".to_owned(), Ok([7; 16])),
        (
            "audit".to_owned(),
            Err(KafkaError::new(ErrorKind::Broker, "rejected")),
        ),
    ]);
    let result = DeleteShareGroupOffsetsResult::new(Duration::from_millis(73), topics);

    assert_eq!(result.throttle_time(), Duration::from_millis(73));
    let (topic, outcome) = &result.topics().entries()[0];
    assert_eq!(topic, "orders");
    assert_eq!(outcome, &Ok([7; 16]));
    assert_eq!(result.into_topics().entries().len(), 2);
}
