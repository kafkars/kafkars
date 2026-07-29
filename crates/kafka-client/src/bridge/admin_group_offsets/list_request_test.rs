//! Inert group-offset bridge request scenarios.

use crate::{StartPosition, TopicPartition, admin::ListConsumerGroupOffsetsQuery};

use super::list_request::{
    ListConsumerGroupOffsetsAdminRequest, ListConsumerGroupsOffsetsAdminRequest,
};

#[test]
fn request_is_linear_sendable_and_retains_group_identity() {
    fn assert_send<T: Send>() {}
    assert_send::<ListConsumerGroupOffsetsAdminRequest>();

    let request = ListConsumerGroupOffsetsAdminRequest::all("payments".to_owned());
    let debug = format!("{request:?}");
    assert!(debug.contains("payments"));
    assert!(debug.contains("All"));
    assert!(debug.contains("require_stable: false"));
}

#[test]
fn singular_and_plural_requests_retain_selection_until_submission() {
    let selected = ListConsumerGroupOffsetsAdminRequest::all("payments".to_owned())
        .with_partitions(vec![
            TopicPartition::new("zeta", 3),
            TopicPartition::new("audit", 1).start_at(StartPosition::Beginning),
        ]);
    assert!(format!("{selected:?}").contains("Selected"));

    let plural = ListConsumerGroupsOffsetsAdminRequest::new(vec![
        ListConsumerGroupOffsetsQuery::selected(
            "payments",
            [
                TopicPartition::new("zeta", 3),
                TopicPartition::new("audit", 1),
            ],
        ),
        ListConsumerGroupOffsetsQuery::all("audit-workers"),
    ]);
    assert!(format!("{plural:?}").contains("query_count: 2"));
}
