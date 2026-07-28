//! Inert bridge request ownership and selection tests.

use kafka_client_engine::{
    ListPartitionReassignmentTarget as EngineTarget,
    ListPartitionReassignmentsRequest as EngineRequest,
};

use crate::{StartPosition, TopicPartition};

use super::request::ListPartitionReassignmentsAdminRequest;

#[test]
fn request_is_linear_and_sendable() {
    fn assert_send<T: Send>() {}
    assert_send::<ListPartitionReassignmentsAdminRequest>();
}

#[test]
fn selected_and_all_active_requests_remain_explicit() {
    let selected =
        ListPartitionReassignmentsAdminRequest::selected(vec![TopicPartition::new("orders", 2)]);
    assert_eq!(
        selected.into_engine(),
        EngineRequest::selected(vec![EngineTarget::new("orders".to_owned(), 2)])
    );
    assert_eq!(
        ListPartitionReassignmentsAdminRequest::all_active().into_engine(),
        EngineRequest::all_active()
    );
}

#[test]
fn assignment_only_start_position_is_preserved_as_invalid_input() {
    let selected = ListPartitionReassignmentsAdminRequest::selected(vec![
        TopicPartition::new("orders", 2).start_at(StartPosition::End),
    ]);
    assert_eq!(
        selected.into_engine(),
        EngineRequest::selected(vec![EngineTarget::new("orders".to_owned(), i32::MIN)])
    );
}
