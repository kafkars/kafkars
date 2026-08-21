//! Inert group-offset deletion bridge request scenarios.

use crate::TopicPartition;

use super::admin_group_offset_delete_request::DeleteConsumerGroupOffsetsAdminRequest;

#[test]
fn request_is_linear_sendable_and_prepared_before_submission() {
    fn assert_send<T: Send>() {}
    assert_send::<DeleteConsumerGroupOffsetsAdminRequest>();

    let request = DeleteConsumerGroupOffsetsAdminRequest::new(
        "payments".to_owned(),
        vec![
            TopicPartition::new("orders", 7),
            TopicPartition::new("audit", 1),
        ],
    );
    assert!(format!("{request:?}").contains("DeleteConsumerGroupOffsetsAdminRequest"));
    let _engine_request = request.into_engine();
}
