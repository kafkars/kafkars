//! Stable `StreamsGroup` scalar and task value tests.

use super::{
    StreamsGroupEndpoint, StreamsGroupKeyValue, StreamsGroupTaskIds, StreamsGroupTaskOffset,
};

#[test]
fn scalar_and_task_values_preserve_exact_facts() {
    let pair = StreamsGroupKeyValue::new("rack".to_owned(), "az-a".to_owned());
    assert_eq!(pair.key(), "rack");
    assert_eq!(pair.value(), "az-a");

    let endpoint = StreamsGroupEndpoint::new("streams.example".to_owned(), 8443);
    assert_eq!(endpoint.host(), "streams.example");
    assert_eq!(endpoint.port(), 8443);

    let offset = StreamsGroupTaskOffset::new("sub-b".to_owned(), 2, 91);
    assert_eq!(offset.subtopology_id(), "sub-b");
    assert_eq!(offset.partition(), 2);
    assert_eq!(offset.offset(), 91);

    let tasks = StreamsGroupTaskIds::new("sub-b".to_owned(), vec![1, 3]);
    assert_eq!(tasks.subtopology_id(), "sub-b");
    assert_eq!(tasks.partitions(), [1, 3]);
}
