//! Synchronous rejection ownership and exact destructive-plan correlation.

use std::time::{Duration, Instant};

use kafka_client_core::{ConsumerGroupMemberRemoval, Moment, RemoveConsumerGroupMembersPlan};

use crate::{
    admin::{
        AdminCompletionNotifier, RemoveConsumerGroupMembersDeliveryStatus,
        RemoveConsumerGroupMembersFailureKind, RemoveConsumerGroupMembersOutcome,
    },
    clock::OperationDeadline,
};

use super::{
    RemoveConsumerGroupMembersHost, RemoveConsumerGroupMembersHostError,
    RemoveConsumerGroupMembersTurn,
};

#[test]
fn synchronous_rejection_preserves_exact_not_sent_settlement() {
    let (mut host, notifier) = host();
    let admission = admit(&mut host, plan("workers", &["a", "b"], Some("drain")));
    let RemoveConsumerGroupMembersTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, plan, scratch, result_limit) = submission.into_parts();

    host.reject_handoff(operation_id, plan, scratch, result_limit)
        .unwrap_or_else(|error| panic!("reject exact submission: {error}"));
    let RemoveConsumerGroupMembersOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe rejection: {error}"))
    else {
        panic!("driver rejection expected");
    };
    assert_eq!(
        failure.into_parts(),
        (
            RemoveConsumerGroupMembersFailureKind::DriverRejected,
            RemoveConsumerGroupMembersDeliveryStatus::NotSent,
        )
    );

    drop(host);
    stop_notifier(notifier);
}

#[test]
fn mismatched_order_and_reason_retain_rejection_and_block_publication() {
    let (mut host, notifier) = host();
    let admission = admit(&mut host, plan("workers", &["a", "b"], Some("drain")));
    let RemoveConsumerGroupMembersTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, _plan, scratch, result_limit) = submission.into_parts();
    let mismatch = plan("workers", &["b", "a"], Some("replace"));

    assert!(matches!(
        host.reject_handoff(operation_id, mismatch, scratch, result_limit),
        Err(RemoveConsumerGroupMembersHostError::SubmissionMismatch)
    ));
    assert!(host.rejected_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(RemoveConsumerGroupMembersHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(notifier);
}

fn admit(
    host: &mut RemoveConsumerGroupMembersHost,
    plan: RemoveConsumerGroupMembersPlan,
) -> super::RemoveConsumerGroupMembersAdmission {
    host.try_admit(Moment::from_tick(1), deadline(), plan)
        .unwrap_or_else(|error| panic!("admit removal: {error:?}"))
}

fn plan(group: &str, members: &[&str], reason: Option<&str>) -> RemoveConsumerGroupMembersPlan {
    RemoveConsumerGroupMembersPlan::new(
        group.to_owned(),
        members
            .iter()
            .map(|member| ConsumerGroupMemberRemoval::new((*member).to_owned()))
            .collect(),
        reason.map(str::to_owned),
    )
    .unwrap_or_else(|error| panic!("plan: {error}"))
}

fn host() -> (RemoveConsumerGroupMembersHost, AdminCompletionNotifier) {
    let (notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("start notifier: {error}"));
    (
        RemoveConsumerGroupMembersHost::new(ports.remove_consumer_group_members),
        notifier,
    )
}

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        kafka_client_core::Deadline::from_tick(10),
        Instant::now() + Duration::from_secs(1),
    )
}

fn stop_notifier(mut notifier: AdminCompletionNotifier) {
    notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"))
        .join_off_notifier()
        .unwrap_or_else(|_| panic!("join notifier"));
}
