//! Per-group consumer offset selection vocabulary tests.

use crate::TopicPartition;

use super::ListConsumerGroupOffsetsQuery;

#[test]
fn query_preserves_all_or_selected_intent_and_caller_order() {
    assert_eq!(
        ListConsumerGroupOffsetsQuery::all("orders").into_parts(),
        ("orders".to_owned(), None)
    );

    let selected = vec![
        TopicPartition::new("zeta", 3),
        TopicPartition::new("audit", 1),
    ];
    assert_eq!(
        ListConsumerGroupOffsetsQuery::selected("workers", selected.clone()).into_parts(),
        ("workers".to_owned(), Some(selected))
    );
}

#[test]
fn string_conversions_preserve_the_existing_all_partition_grammar() {
    assert_eq!(
        ListConsumerGroupOffsetsQuery::from("borrowed").into_parts(),
        ("borrowed".to_owned(), None)
    );
    assert_eq!(
        ListConsumerGroupOffsetsQuery::from("owned".to_owned()).into_parts(),
        ("owned".to_owned(), None)
    );
}
