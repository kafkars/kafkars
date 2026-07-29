//! Exact generated API-92 v0 request construction scenarios.

use kafka_client_core::DeleteShareGroupOffsetsPlan;

use super::delete_share_group_offsets_request;

#[test]
fn request_preserves_group_and_caller_topic_order() {
    let plan = DeleteShareGroupOffsetsPlan::new(
        "share-readers".to_owned(),
        vec!["zeta".to_owned(), "alpha".to_owned()],
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"));

    let request = delete_share_group_offsets_request(&plan)
        .unwrap_or_else(|failure| panic!("request allocation: {failure:?}"));

    assert_eq!(request.group_id.as_str(), "share-readers");
    assert_eq!(request.topics.len(), 2);
    assert_eq!(request.topics[0].topic_name.as_str(), "zeta");
    assert_eq!(request.topics[1].topic_name.as_str(), "alpha");
}
