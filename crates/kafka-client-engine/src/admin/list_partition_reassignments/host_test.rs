//! Admission, deadline, recovery, and retained-byte scenarios.

use std::time::Instant;

use core::mem::size_of;

use kafka_client_core::{
    ListPartitionReassignmentTarget, ListPartitionReassignmentsMachineError,
    ListPartitionReassignmentsPlan, Moment,
};

use crate::{
    admin::{AdminCompletionNotifier, ListPartitionReassignmentsHost},
    clock::OperationDeadline,
};

use super::{
    ListPartitionReassignmentsAdmissionErrorKind, ListPartitionReassignmentsDeliveryStatus,
    ListPartitionReassignmentsFailureKind, ListPartitionReassignmentsHostError,
    ListPartitionReassignmentsOutcome, ListPartitionReassignmentsTurn,
    host::LIST_PARTITION_REASSIGNMENTS_RETAINED_BYTES,
};

#[test]
fn admission_reserves_terminal_and_complete_envelope_before_submission() {
    let (mut host, notifier) = host();
    let deadline = deadline(10);
    let Ok(admission) = host.try_admit(
        Moment::from_tick(1),
        deadline,
        ListPartitionReassignmentsPlan::all_active(),
    ) else {
        panic!("all-active query admission expected");
    };
    assert!(admission.fault.is_none());
    assert_eq!(
        host.retained_bytes_for_test(),
        LIST_PARTITION_REASSIGNMENTS_RETAINED_BYTES
    );
    assert_eq!(host.next_deadline(), Some(deadline.core()));
    assert!(matches!(
        host.try_admit(
            Moment::from_tick(1),
            deadline,
            ListPartitionReassignmentsPlan::all_active(),
        ),
        Err(ListPartitionReassignmentsAdmissionErrorKind::RetainedBytes)
    ));
    let Ok(turn) = host.turn(Moment::from_tick(2)) else {
        panic!("submission turn expected");
    };
    let ListPartitionReassignmentsTurn::Submit(submission) = turn else {
        panic!("submission expected");
    };
    let (_operation_id, submitted_deadline, submitted_plan, result_limit) = submission.into_parts();
    assert_eq!(submitted_deadline, deadline);
    assert_eq!(submitted_plan, ListPartitionReassignmentsPlan::all_active());
    assert!(result_limit < LIST_PARTITION_REASSIGNMENTS_RETAINED_BYTES);
    assert!(result_limit > LIST_PARTITION_REASSIGNMENTS_RETAINED_BYTES / 2);

    drop((admission, host));
    stop_notifier(notifier);
}

#[test]
fn untouched_shutdown_recovery_is_definitely_unsent() {
    let (mut host, notifier) = host();
    let Ok(admission) = host.try_admit(
        Moment::from_tick(1),
        deadline(10),
        ListPartitionReassignmentsPlan::all_active(),
    ) else {
        panic!("all-active query admission expected");
    };
    let Ok(()) = host.recover_after_driver_shutdown() else {
        panic!("host recovery expected");
    };
    let Ok(outcome) = admission.observer.wait() else {
        panic!("recovery observation expected");
    };
    let ListPartitionReassignmentsOutcome::Failed(failure) = outcome else {
        panic!("recovery failure expected");
    };
    let (kind, delivery) = failure.into_parts();
    assert_eq!(
        (kind, delivery),
        (
            ListPartitionReassignmentsFailureKind::DriverRejected,
            ListPartitionReassignmentsDeliveryStatus::NotSent,
        )
    );
    let Ok(_progress) = host.turn(Moment::from_tick(2)) else {
        panic!("terminal reclaim expected");
    };
    assert_eq!(host.retained_bytes_for_test(), 0);

    drop(host);
    stop_notifier(notifier);
}

#[test]
fn selected_admission_charges_every_retained_target_owner() {
    let all_active_limit = submitted_result_limit(ListPartitionReassignmentsPlan::all_active());
    let targets = vec![
        ListPartitionReassignmentTarget::new("a".to_owned(), 1),
        ListPartitionReassignmentTarget::new("long-topic".to_owned(), 2),
    ];
    let target_count = targets.len();
    let topic_bytes = targets
        .iter()
        .map(|target| target.topic().len())
        .sum::<usize>();
    let selected = ListPartitionReassignmentsPlan::selected(targets)
        .unwrap_or_else(|error| panic!("valid selected plan: {error}"));
    let selected_limit = submitted_result_limit(selected);

    assert_eq!(
        all_active_limit - selected_limit,
        3 * (target_count * size_of::<ListPartitionReassignmentTarget>() + topic_bytes)
    );
}

#[test]
fn recovered_call_remains_retained_when_core_rejects_terminal_fact() {
    let (mut host, notifier) = host();
    let Ok(admission) = host.try_admit(
        Moment::from_tick(1),
        deadline(10),
        ListPartitionReassignmentsPlan::all_active(),
    ) else {
        panic!("all-active query admission expected");
    };
    host.retain_recovered_call_for_test();

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(ListPartitionReassignmentsHostError::Machine(
            ListPartitionReassignmentsMachineError::InvalidState
        ))
    ));
    assert!(host.recovered_call_is_retained_for_test());

    drop((admission, host));
    stop_notifier(notifier);
}

fn submitted_result_limit(plan: ListPartitionReassignmentsPlan) -> usize {
    let (mut host, notifier) = host();
    let Ok(admission) = host.try_admit(Moment::from_tick(1), deadline(10), plan) else {
        panic!("query admission expected");
    };
    let Ok(ListPartitionReassignmentsTurn::Submit(submission)) = host.turn(Moment::from_tick(2))
    else {
        panic!("submission expected");
    };
    let (_operation_id, _deadline, _plan, result_limit) = submission.into_parts();
    drop((admission, host));
    stop_notifier(notifier);
    result_limit
}

fn host() -> (ListPartitionReassignmentsHost, AdminCompletionNotifier) {
    let (notifier, ports) = AdminCompletionNotifier::start()
        .unwrap_or_else(|error| panic!("start shared admin notifier: {error}"));
    (
        ListPartitionReassignmentsHost::new(ports.list_partition_reassignments),
        notifier,
    )
}

fn stop_notifier(mut notifier: AdminCompletionNotifier) {
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop admin notifier: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("join admin notifier: {error}"));
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(tick),
        Instant::now() + std::time::Duration::from_secs(1),
    )
}
