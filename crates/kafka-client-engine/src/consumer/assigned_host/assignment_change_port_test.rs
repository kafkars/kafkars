//! Port forwarding and empty-change wake suppression scenarios.

use std::time::Duration;

use super::{AssignedConsumerPartition, result::AssignedConsumerPortError, shard_test::setup};

#[test]
fn port_preserves_no_assignment_and_closed_rejections() {
    let (owner, port, wake) = setup();
    let target = AssignedConsumerPartition::try_new("orders", 0)
        .unwrap_or_else(|error| panic!("valid target: {error}"));
    let error = port
        .remove_assignments(vec![target])
        .err()
        .unwrap_or_else(|| panic!("nonempty unassigned removal must reject"));
    assert!(matches!(
        error,
        AssignedConsumerPortError::Owner {
            error: super::super::assigned_owner_model::AssignedConsumerOwnerError::Core(
                kafka_client_core::AssignedConsumerMachineError::NoAssignment
            ),
            wake: None,
        }
    ));
    assert_eq!(wake.count(), 0);

    owner
        .close_assigned_admission()
        .unwrap_or_else(|error| panic!("close admission: {error:?}"));
    let capture = port
        .capture_assignment_deadline(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture deadline: {error:?}"));
    assert!(matches!(
        port.add_assignments_captured(Vec::new(), capture),
        Err(AssignedConsumerPortError::Closed)
    ));
}
