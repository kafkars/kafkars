//! Exact rejection, accepted-call, and recovered-call correlation scenarios.

use crate::{
    EngineConfig,
    admin::AdminCompletionNotifier,
    driver::{DriverOwner, UpdateFeaturesCall},
};

use super::super::{
    UpdateFeaturesHost, UpdateFeaturesHostError, UpdateFeaturesTurn,
    host_test::{deadline, plan, stop_notifier},
};

#[test]
fn mismatched_rejection_evidence_blocks_settlement() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = UpdateFeaturesHost::new(ports.update_features);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(false))
        .unwrap_or_else(|error| panic!("admit feature update: {error:?}"));
    let UpdateFeaturesTurn::Submit(submission) = host
        .turn(capture.now(), None)
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, _deadline, submitted_plan, result_limit) = submission.into_parts();

    assert!(matches!(
        host.reject_handoff(operation_id, plan(true), result_limit),
        Err(UpdateFeaturesHostError::SubmissionMismatch)
    ));
    host.reject_handoff(operation_id, submitted_plan, result_limit)
        .unwrap_or_else(|error| panic!("exact rejection: {error}"));

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn mismatched_accepted_call_remains_owned_and_blocks_recovery() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = UpdateFeaturesHost::new(ports.update_features);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(false))
        .unwrap_or_else(|error| panic!("admit feature update: {error:?}"));
    let UpdateFeaturesTurn::Submit(submission) = host
        .turn(capture.now(), None)
        .unwrap_or_else(|error| panic!("take submission: {error}"))
    else {
        panic!("submission expected");
    };
    let (operation_id, submitted_deadline, _submitted_plan, result_limit) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let call = UpdateFeaturesCall::submit(
        &driver,
        plan(true),
        result_limit,
        submitted_deadline,
        capture.now(),
    )
    .unwrap_or_else(|error| panic!("accepted call: {error}"));

    assert!(matches!(
        host.accept_call(operation_id, call),
        Err(UpdateFeaturesHostError::SubmissionMismatch)
    ));
    assert!(host.call_ownership_is_retained_for_test());
    drop(driver);
    assert!(matches!(
        host.recover_after_driver_shutdown(),
        Err(UpdateFeaturesHostError::SubmissionMismatch)
    ));
    assert!(host.recovered_call_is_retained_for_test());

    drop((admission, host));
    stop_notifier(&mut notifier);
}

#[test]
fn mismatched_recovered_evidence_blocks_terminal_settlement() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = UpdateFeaturesHost::new(ports.update_features);
    let capture = deadline();
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan(false))
        .unwrap_or_else(|error| panic!("admit feature update: {error:?}"));
    host.retain_mismatched_recovered_call_for_test();

    assert!(matches!(
        host.settle_recovered_transport_for_test(),
        Err(UpdateFeaturesHostError::SubmissionMismatch)
    ));
    assert!(host.recovered_call_is_retained_for_test());
    assert!(matches!(
        host.publish_terminal_for_test(),
        Err(UpdateFeaturesHostError::InvalidHandoff)
    ));

    drop((admission, host));
    stop_notifier(&mut notifier);
}
