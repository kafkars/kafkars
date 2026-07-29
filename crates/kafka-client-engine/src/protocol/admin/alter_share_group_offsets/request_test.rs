//! Exact grouping and scalar preservation for generated API-91 v0 requests.

use kafka_client_core::{AlterShareGroupOffset, AlterShareGroupOffsetsPlan};

use super::alter_share_group_offsets_request;

#[test]
fn request_groups_first_topic_appearance_and_keeps_partition_order_and_offsets() {
    let plan = AlterShareGroupOffsetsPlan::new(
        "share-readers".to_owned(),
        vec![
            AlterShareGroupOffset::new("orders".to_owned(), 2, 52),
            AlterShareGroupOffset::new("audit".to_owned(), 1, 7),
            AlterShareGroupOffset::new("orders".to_owned(), 0, 50),
        ],
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"));

    let request = alter_share_group_offsets_request(&plan)
        .unwrap_or_else(|failure| panic!("request allocation: {failure:?}"));

    assert_eq!(request.group_id.as_str(), "share-readers");
    assert_eq!(request.topics.len(), 2);
    assert_eq!(request.topics[0].topic_name.as_str(), "orders");
    assert_eq!(request.topics[0].partitions.len(), 2);
    assert_eq!(request.topics[0].partitions[0].partition_index, 2);
    assert_eq!(request.topics[0].partitions[0].start_offset, 52);
    assert_eq!(request.topics[0].partitions[1].partition_index, 0);
    assert_eq!(request.topics[0].partitions[1].start_offset, 50);
    assert_eq!(request.topics[1].topic_name.as_str(), "audit");
}
