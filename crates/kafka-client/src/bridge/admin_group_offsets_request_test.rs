//! Inert group-offset bridge request scenarios.

use super::admin_group_offsets_request::ListConsumerGroupOffsetsAdminRequest;

#[test]
fn request_is_linear_sendable_and_retains_group_identity() {
    fn assert_send<T: Send>() {}
    assert_send::<ListConsumerGroupOffsetsAdminRequest>();

    let request = ListConsumerGroupOffsetsAdminRequest::new("payments".to_owned());
    let debug = format!("{request:?}");
    assert!(debug.contains("payments"));
    assert!(debug.contains("require_stable: false"));
}
