//! Causal stale-controller classification for normalized API 45 terminals.

use core::num::NonZeroI16;

use kafka_client_core::{
    AlterPartitionReassignmentBrokerError, AlterPartitionReassignmentOutcome,
    AlterPartitionReassignmentsBatch, AlterPartitionReassignmentsInput,
};

use super::alter_partition_reassignments_terminal::input_requires_controller_refresh;

#[test]
fn exact_top_level_not_controller_requires_refresh() {
    assert!(input_requires_controller_refresh(&broker_rejected(41)));
    assert!(!input_requires_controller_refresh(&broker_rejected(42)));
}

#[test]
fn exact_partition_not_controller_requires_refresh() {
    let batch = AlterPartitionReassignmentsBatch::new(
        0,
        vec![AlterPartitionReassignmentOutcome::failed(
            "orders".to_owned(),
            0,
            broker_error(41),
        )],
    );
    assert!(input_requires_controller_refresh(
        &AlterPartitionReassignmentsInput::BrokerResponded { batch }
    ));
    assert!(!input_requires_controller_refresh(
        &AlterPartitionReassignmentsInput::InvalidResponse
    ));
}

fn broker_rejected(code: i16) -> AlterPartitionReassignmentsInput {
    AlterPartitionReassignmentsInput::BrokerRejected {
        error: broker_error(code),
    }
}

fn broker_error(code: i16) -> AlterPartitionReassignmentBrokerError {
    AlterPartitionReassignmentBrokerError::with_bounded_message(
        NonZeroI16::new(code).unwrap_or_else(|| panic!("test code must be nonzero")),
        None,
        false,
    )
}
