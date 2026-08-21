//! Per-group Streams offset selection vocabulary tests.

use crate::TopicPartition;

use super::ListStreamsGroupOffsetsQuery;

#[test]
fn query_preserves_all_or_selected_intent_and_caller_order() {
    let all = ListStreamsGroupOffsetsQuery::all("streams-all").into_consumer_group();
    assert_eq!(all.into_parts(), ("streams-all".to_owned(), None));

    let selected = vec![
        TopicPartition::new("orders", 2),
        TopicPartition::new("audit", 0),
    ];
    let selected_query =
        ListStreamsGroupOffsetsQuery::selected("streams-selected", selected.clone())
            .into_consumer_group();
    assert_eq!(
        selected_query.into_parts(),
        ("streams-selected".to_owned(), Some(selected))
    );
}

#[test]
fn borrowed_string_preserves_the_existing_all_partition_grammar() {
    let query = ListStreamsGroupOffsetsQuery::from("streams-workers").into_consumer_group();
    assert_eq!(query.into_parts(), ("streams-workers".to_owned(), None));
}
