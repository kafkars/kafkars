//! Completion-error ownership remains installed until post-driver recovery.

use std::time::{Duration, Instant};

use kafka_client_core::{
    ConsumerGroupMemberRemoval, Deadline, Moment, RemoveConsumerGroupMembersPlan,
};

use crate::{
    EngineConfig,
    admin::{AdminCompletionNotifier, RemoveConsumerGroupMembersHost},
    clock::OperationDeadline,
    driver::{DriverOwner, RemoveConsumerGroupMembersCall},
};

use super::{
    RemoveConsumerGroupMembersDeliveryStatus, RemoveConsumerGroupMembersFailureKind,
    RemoveConsumerGroupMembersHostError, RemoveConsumerGroupMembersOutcome,
    RemoveConsumerGroupMembersTurn,
};

#[test]
fn completion_fault_retains_call_until_post_driver_recovery() {
    let (mut host, notifier) = host();
    let deadline = deadline();
    let admission = host
        .try_admit(Moment::from_tick(1), deadline, plan())
        .unwrap_or_else(|error| panic!("admission: {error:?}"));
    let RemoveConsumerGroupMembersTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, submitted_plan, scratch, result_limit) =
        submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = RemoveConsumerGroupMembersCall::submit(
        &driver,
        submitted_plan.clone(),
        scratch,
        result_limit,
        submitted_deadline,
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"));
    host.accept_call(operation_id, call)
        .unwrap_or_else(|error| panic!("host acceptance: {error}"));
    drop(driver);

    assert!(matches!(
        host.turn(Moment::from_tick(3)),
        Err(RemoveConsumerGroupMembersHostError::CallCompletion)
    ));
    assert!(host.call_matches_for_test(&submitted_plan, scratch, result_limit));
    host.recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("post-driver recovery: {error}"));
    let RemoveConsumerGroupMembersOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("recovery observation: {error}"))
    else {
        panic!("recovery failure expected");
    };
    assert_eq!(
        failure.into_parts(),
        (
            RemoveConsumerGroupMembersFailureKind::Transport,
            RemoveConsumerGroupMembersDeliveryStatus::PossiblySent,
        )
    );

    drop(host);
    stop_notifier(notifier);
}

fn plan() -> RemoveConsumerGroupMembersPlan {
    RemoveConsumerGroupMembersPlan::new(
        "workers".to_owned(),
        vec![ConsumerGroupMemberRemoval::new("instance-a".to_owned())],
        None,
    )
    .unwrap_or_else(|error| panic!("plan: {error}"))
}

fn host() -> (RemoveConsumerGroupMembersHost, AdminCompletionNotifier) {
    let (notifier, ports) = AdminCompletionNotifier::start()
        .unwrap_or_else(|error| panic!("start shared admin notifier: {error}"));
    (
        RemoveConsumerGroupMembersHost::new(ports.remove_consumer_group_members),
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

fn deadline() -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        Deadline::from_tick(10),
        Instant::now() + Duration::from_secs(1),
    )
}
