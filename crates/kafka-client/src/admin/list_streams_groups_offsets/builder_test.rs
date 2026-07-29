//! Multi-Streams-group builder type guarantees.

use crate::{
    Client, DeliveryStatus, ErrorKind, StartPosition, TopicPartition,
    admin::ListStreamsGroupOffsetsQuery,
};

use super::ListStreamsGroupsOffsetsBuilder;

#[test]
fn builder_is_send_before_single_submission() {
    fn assert_send<T: Send>() {}
    assert_send::<ListStreamsGroupsOffsetsBuilder>();
}

#[test]
fn explicit_selected_query_defers_assignment_position_rejection_until_submit() {
    let error = Client::builder()
        .bootstrap_servers(["127.0.0.1:9092"])
        .build()
        .unwrap_or_else(|error| panic!("build test client: {error}"))
        .admin()
        .list_streams_groups_offsets([
            ListStreamsGroupOffsetsQuery::all("streams-orders"),
            ListStreamsGroupOffsetsQuery::selected(
                "streams-audit",
                [TopicPartition::new("audit", 0).start_at(StartPosition::Beginning)],
            ),
        ])
        .submit()
        .wait()
        .expect_err("assignment-only start position must reject at submit");
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}
