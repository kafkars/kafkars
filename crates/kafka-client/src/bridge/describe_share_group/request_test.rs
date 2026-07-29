//! Inert ShareGroup description request bridge tests.

use super::{engine::Request as EngineRequest, request::DescribeShareGroupAdminRequest};

#[test]
fn request_is_linear_sendable_and_preserves_authorization_intent() {
    fn assert_send<T: Send>() {}
    assert_send::<DescribeShareGroupAdminRequest>();

    assert_eq!(
        DescribeShareGroupAdminRequest::new("share-workers".to_owned()).into_engine(),
        EngineRequest::new("share-workers".to_owned()),
    );

    let mut request = DescribeShareGroupAdminRequest::new("share-workers".to_owned());
    request.set_include_authorized_operations(true);
    assert!(format!("{request:?}").contains("share-workers"));
    assert_eq!(
        request.into_engine(),
        EngineRequest::new("share-workers".to_owned()).with_authorized_operations(true),
    );
}
