//! Linear incremental-assignment deadline-capture scenarios.

use std::{sync::Arc, time::Duration};

use super::{
    AssignedConsumerAssignment, AssignedConsumerHandle, AssignedConsumerStartPosition,
    AssignedConsumerTryChangeAssignmentErrorKind, claim::AssignedConsumerClaimSlot,
    shard_test::setup,
};

#[test]
fn captured_addition_deadline_stays_bound_across_later_contention() {
    let (owner, port, _wake) = setup();
    let mut handle = claim(port);
    let capture = handle
        .capture_add_assignments(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("capture addition: {error}"));
    let guard = owner.lock_for_test();
    let error = capture
        .try_add_assignments(vec![assignment("orders", 0)])
        .err()
        .unwrap_or_else(|| panic!("contended addition must reject"));
    drop(guard);

    assert_eq!(
        error.kind(),
        AssignedConsumerTryChangeAssignmentErrorKind::Contended
    );
}

fn claim(port: super::AssignedConsumerPort) -> AssignedConsumerHandle {
    let (slot, _closer) = AssignedConsumerClaimSlot::create_for_engine(port);
    let lifetime: Arc<dyn Send + Sync> = Arc::new(());
    slot.claim(lifetime)
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"))
}

fn assignment(topic: &str, partition: i32) -> AssignedConsumerAssignment {
    AssignedConsumerAssignment::try_new(topic, partition, AssignedConsumerStartPosition::Beginning)
        .unwrap_or_else(|error| panic!("valid assignment: {error}"))
}
