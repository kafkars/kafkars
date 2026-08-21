//! Inert `StreamsGroup` description request bridge tests.

use super::{engine::Request as EngineRequest, request::DescribeStreamsGroupAdminRequest};

#[test]
fn request_is_linear_sendable_and_preserves_both_optional_intents() {
    fn assert_send<T: Send>() {}
    assert_send::<DescribeStreamsGroupAdminRequest>();

    assert_eq!(
        DescribeStreamsGroupAdminRequest::new("streams-workers".to_owned()).into_engine(),
        EngineRequest::new("streams-workers".to_owned()),
    );

    let mut request = DescribeStreamsGroupAdminRequest::new("streams-workers".to_owned());
    request.set_include_authorized_operations(true);
    request.set_include_topology_description(true);
    assert!(format!("{request:?}").contains("streams-workers"));
    assert_eq!(
        request.into_engine(),
        EngineRequest::new("streams-workers".to_owned())
            .with_authorized_operations(true)
            .with_topology_description(true),
    );
}
