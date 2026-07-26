//! Immutable read-isolation ownership scenarios.

use super::{AssignedConsumerMachine, ReadIsolation};

#[test]
fn default_preserves_existing_read_uncommitted_policy() {
    assert_eq!(
        AssignedConsumerMachine::new().read_isolation(),
        ReadIsolation::ReadUncommitted
    );
}

#[test]
fn construction_retains_one_immutable_read_isolation() {
    let machine = AssignedConsumerMachine::with_read_isolation(ReadIsolation::ReadCommitted);

    assert_eq!(machine.read_isolation(), ReadIsolation::ReadCommitted);
}
