//! Inert bridge request ownership and exact change translation.

use kafka_client_engine::{
    AlterPartitionReassignmentsRequest as EngineRequest,
    PartitionReassignmentChange as EngineChange,
};

use crate::PartitionReassignmentChange;

use super::alter_request::AlterPartitionReassignmentsAdminRequest;

#[test]
fn request_is_linear_sendable_and_preserves_change_order() {
    fn assert_send<T: Send>() {}
    assert_send::<AlterPartitionReassignmentsAdminRequest>();

    let request = AlterPartitionReassignmentsAdminRequest::new(vec![
        PartitionReassignmentChange::new("orders", 2, [7, 3]),
        PartitionReassignmentChange::cancel("audit", 0),
    ]);
    assert_eq!(
        request.into_engine(),
        EngineRequest::new(vec![
            EngineChange::replace("orders".to_owned(), 2, vec![7, 3]),
            EngineChange::cancel("audit".to_owned(), 0),
        ])
    );
}
