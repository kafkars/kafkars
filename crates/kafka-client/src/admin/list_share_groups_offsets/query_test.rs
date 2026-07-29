//! Per-group ShareGroup offset selection tests.

use crate::TopicPartition;

use super::ListShareGroupOffsetsQuery;

#[test]
fn query_preserves_all_or_selected_intent() {
    assert_eq!(
        ListShareGroupOffsetsQuery::all("orders").into_parts(),
        ("orders".to_owned(), None)
    );
    let selected = vec![TopicPartition::new("orders", 2)];
    assert_eq!(
        ListShareGroupOffsetsQuery::selected("audit", selected.clone()).into_parts(),
        ("audit".to_owned(), Some(selected))
    );
}
