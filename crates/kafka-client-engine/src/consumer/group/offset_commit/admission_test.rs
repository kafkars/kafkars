//! Admission, rollback, deadline, and observer-abandonment scenarios.

use super::{
    host::{GroupOffsetCommitHost, GroupOffsetCommitHostError},
    test_support::{catalog, checkpoint, deadline},
};

#[test]
fn admission_reserves_terminal_result_bytes_and_original_deadline() {
    let catalog = catalog();
    let checkpoint = checkpoint(&catalog);
    let expected_deadline = deadline(91);
    let mut host =
        GroupOffsetCommitHost::start().unwrap_or_else(|error| panic!("host start: {error}"));
    let admission = host
        .try_admit(&catalog, expected_deadline, checkpoint)
        .unwrap_or_else(|failure| panic!("admission failed: {:?}", failure.kind));

    assert!(admission.fault.is_none());
    assert_eq!(host.operations.len(), 1);
    assert_eq!(host.operations[0].deadline, expected_deadline);
    assert_eq!(
        host.operations[0].machine.deadline(),
        expected_deadline.core()
    );
    assert!(matches!(
        host.operations[0].attempt,
        Some(super::host::GroupOffsetCommitAttempt::Queued(_))
    ));
    assert_eq!(
        host.retained_bytes_for_test(),
        host.operations[0].byte_charge
    );

    drop(admission.observer);
    assert_eq!(
        host.operations.len(),
        1,
        "observer drop is not cancellation"
    );
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recovery: {error}"));
    let join = host
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("finish shutdown: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}

#[test]
fn retained_host_fault_rejects_new_admission_with_the_exact_checkpoint() {
    let catalog = catalog();
    let checkpoint = checkpoint(&catalog);
    let expected_generation = checkpoint.assignment_generation();
    let mut host =
        GroupOffsetCommitHost::start().unwrap_or_else(|error| panic!("host start: {error}"));
    host.fault = Some(GroupOffsetCommitHostError::Preparation);

    let failure = host
        .try_admit(&catalog, deadline(40), checkpoint)
        .err()
        .unwrap_or_else(|| panic!("faulted host must reject"));

    assert_eq!(
        failure.kind,
        super::admission::GroupOffsetCommitAdmissionFailureKind::HostUnavailable
    );
    assert_eq!(
        failure.checkpoint.assignment_generation(),
        expected_generation
    );
    assert!(host.operations.is_empty());
    assert_eq!(host.retained_bytes_for_test(), 0);

    host.fault = None;
    host.close_admission();
    let join = host
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("finish shutdown: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}
