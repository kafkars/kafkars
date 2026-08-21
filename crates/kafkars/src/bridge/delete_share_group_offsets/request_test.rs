//! Inert `ShareGroup` offset-deletion request bridge tests.

use super::request::DeleteShareGroupOffsetsAdminRequest;

#[test]
fn request_is_linear_sendable_and_keeps_invalid_intent_until_submission() {
    fn assert_send<T: Send>() {}
    assert_send::<DeleteShareGroupOffsetsAdminRequest>();

    let request = DeleteShareGroupOffsetsAdminRequest::new(
        String::new(),
        vec!["orders".to_owned(), String::new()],
    );
    let diagnostic = format!("{request:?}");
    assert!(diagnostic.contains("orders"));
    let _engine_request = request.into_engine();
}
