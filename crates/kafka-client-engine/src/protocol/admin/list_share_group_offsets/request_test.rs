//! Nullable-all and first-topic-appearance selected request scenarios.

use kafka_client_core::{ListShareGroupOffsetTarget, ListShareGroupOffsetsPlan};

use super::list_share_group_offsets_request;

#[test]
fn all_selection_writes_one_group_with_null_topics() {
    let plan = ListShareGroupOffsetsPlan::all("share-readers".to_owned())
        .unwrap_or_else(|error| panic!("valid all plan: {error}"));
    let request =
        list_share_group_offsets_request(&plan).unwrap_or_else(|error| panic!("request: {error}"));

    assert_eq!(request.groups.len(), 1);
    assert_eq!(request.groups[0].group_id.as_str(), "share-readers");
    assert_eq!(request.groups[0].topics, None);
}

#[test]
fn selected_partitions_group_by_first_topic_appearance_and_keep_partition_order() {
    let plan = ListShareGroupOffsetsPlan::selected(
        "share-readers".to_owned(),
        vec![
            ListShareGroupOffsetTarget::new("orders".to_owned(), 2),
            ListShareGroupOffsetTarget::new("audit".to_owned(), 1),
            ListShareGroupOffsetTarget::new("orders".to_owned(), 0),
        ],
    )
    .unwrap_or_else(|error| panic!("valid selected plan: {error}"));
    let request =
        list_share_group_offsets_request(&plan).unwrap_or_else(|error| panic!("request: {error}"));
    let topics = request.groups[0]
        .topics
        .as_ref()
        .unwrap_or_else(|| panic!("selected topics"));

    assert_eq!(topics.len(), 2);
    assert_eq!(topics[0].topic_name.as_str(), "orders");
    assert_eq!(topics[0].partitions, [2, 0]);
    assert_eq!(topics[1].topic_name.as_str(), "audit");
    assert_eq!(topics[1].partitions, [1]);
}
