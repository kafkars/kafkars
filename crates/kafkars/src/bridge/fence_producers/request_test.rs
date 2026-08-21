//! Inert caller-ordered producer-fencing request bridge tests.

use super::{engine::Request as EngineRequest, request::FenceProducersAdminRequest};

#[test]
fn request_is_sendable_and_transfers_order_without_per_id_conversion() {
    fn assert_send<T: Send>() {}
    assert_send::<FenceProducersAdminRequest>();

    let request =
        FenceProducersAdminRequest::new(vec!["orders-tx".to_owned(), "audit-tx".to_owned()]);
    assert_eq!(request.transactional_id_count(), 2);
    assert_eq!(
        request.into_engine(),
        EngineRequest::new(vec!["orders-tx".to_owned(), "audit-tx".to_owned()])
    );
}
