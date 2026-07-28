//! Missing and mismatched destructive-attempt recovery scenarios.

use std::time::{Duration, Instant};

use kafka_client_core::{
    ConsumerGroupMemberRemoval, Moment, RemoveConsumerGroupMembersInput,
    RemoveConsumerGroupMembersPlan,
};

use crate::{
    EngineConfig,
    admin::AdminCompletionNotifier,
    clock::OperationDeadline,
    driver::{DriverOwner, RemoveConsumerGroupMembersCall, RemoveConsumerGroupMembersTerminal},
};

use super::super::super::{
    RemoveConsumerGroupMembersHost, RemoveConsumerGroupMembersHostError,
    RemoveConsumerGroupMembersTurn,
};

#[test]
fn handed_off_operation_without_a_call_cannot_forge_shutdown_settlement() {
    let (mut host, notifier) = host();
    let admission = admit(&mut host, plan(&["a", "b"], Some("drain")));
    let RemoveConsumerGroupMembersTurn::Submit(_submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };

    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(RemoveConsumerGroupMembersHostError::InvalidHandoff)
    ));
    assert!(host.publish_terminal_for_test().is_err());

    drop((admission, host));
    stop_notifier(notifier);
}

#[test]
fn mismatched_accepted_order_survives_recovery_and_blocks_publication() {
    let (mut host, notifier) = host();
    let admission = admit(&mut host, plan(&["a", "b"], Some("drain")));
    let RemoveConsumerGroupMembersTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, deadline, _expected, scratch, result_limit) = submission.into_parts();
    let mismatch = plan(&["b", "a"], Some("drain"));
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = RemoveConsumerGroupMembersCall::submit(
        &driver,
        mismatch.clone(),
        scratch,
        result_limit,
        deadline,
    )
    .unwrap_or_else(|error| panic!("accepted mismatched call: {error}"));

    assert!(matches!(
        host.accept_call(operation_id, call),
        Err(RemoveConsumerGroupMembersHostError::SubmissionMismatch)
    ));
    drop(driver);
    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(RemoveConsumerGroupMembersHostError::SubmissionMismatch)
    ));
    assert!(host.recovered_matches_for_test(&mismatch, scratch, result_limit));
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(RemoveConsumerGroupMembersHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(notifier);
}

#[test]
fn mismatched_raw_reason_cannot_settle_core_or_publish() {
    let (mut host, notifier) = host();
    let admission = admit(&mut host, plan(&["a", "b"], Some("drain")));
    let RemoveConsumerGroupMembersTurn::Submit(submission) = host
        .turn(Moment::from_tick(2))
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, _expected, scratch, result_limit) = submission.into_parts();
    host.apply_input_for_test(
        operation_id,
        RemoveConsumerGroupMembersInput::DriverAccepted,
    )
    .unwrap_or_else(|error| panic!("accept synthetic driver ownership: {error}"));
    host.retain_raw_for_test(RemoveConsumerGroupMembersTerminal::for_test(
        plan(&["a", "b"], Some("replace")),
        scratch,
        result_limit,
    ));

    assert!(matches!(
        host.settle_raw_for_test(),
        Err(RemoveConsumerGroupMembersHostError::SubmissionMismatch)
    ));
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
) -> super::super::RemoveConsumerGroupMembersAdmission {
    host.try_admit(Moment::from_tick(1), deadline(), plan)
        .unwrap_or_else(|error| panic!("admit removal: {error:?}"))
}

fn plan(members: &[&str], reason: Option<&str>) -> RemoveConsumerGroupMembersPlan {
    RemoveConsumerGroupMembersPlan::new(
        "workers".to_owned(),
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
