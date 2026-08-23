//! Public acknowledgement admission, publication, recovery, and reclamation evidence.

use std::time::Duration;

use kafka_client_core::Moment;

use crate::{
    EngineConfig,
    consumer::{
        ShareAcknowledgeDeliveryStatus, ShareAcknowledgeOutcome,
        ShareAcknowledgementAdmissionErrorKind,
    },
    driver::{
        DriverOwner,
        share_acknowledge::{ShareAcknowledgeResolution, ShareAcknowledgeRoute},
    },
    protocol::consumer::share_acknowledge::{
        ShareAcknowledgePartitionOutcome, ShareAcknowledgeSuccess,
    },
};

use super::{
    fetch_acknowledgement::ShareAcknowledgementTerminal,
    fetch_session_set::{ShareFetchSessionSetTurn, owner_test::first_session_mut_for_test},
    registry_acknowledgement::ShareAcknowledgementCompletionTurn,
    registry_delivery_test::{finish, staged_handle},
};

#[test]
fn accepted_success_publishes_exact_partition_outcome_then_reclaims_capacity() {
    let (owner, mut handle, group_id) = staged_handle();
    let acknowledgement = handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("take: {error}"))
        .unwrap_or_else(|| panic!("batch"))
        .accept_all()
        .unwrap_or_else(|error| panic!("acknowledgement: {error}"));
    let observer = handle
        .try_acknowledge(acknowledgement, Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("admission: {error}"))
        .into_observer();

    settle_success(&owner, group_id);
    assert_eq!(
        publication_turn(&owner),
        ShareAcknowledgementCompletionTurn::Progress
    );
    let ShareAcknowledgeOutcome::Responded(response) = observer
        .wait()
        .unwrap_or_else(|error| panic!("observe: {error}"))
    else {
        panic!("success response");
    };
    assert_eq!(response.throttle_time_ms(), 7);
    let partitions = response.partitions().collect::<Vec<_>>();
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0].topic_id(), [7; 16]);
    assert_eq!(partitions[0].partition(), 0);
    assert_eq!(partitions[0].broker_code(), None);
    assert_eq!(
        publication_turn(&owner),
        ShareAcknowledgementCompletionTurn::Progress
    );
    finish(owner, group_id);
}

#[test]
fn invalid_deadline_returns_the_exact_capability_without_admission() {
    let (owner, mut handle, group_id) = staged_handle();
    let acknowledgement = handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("take: {error}"))
        .unwrap_or_else(|| panic!("batch"))
        .accept_all()
        .unwrap_or_else(|error| panic!("acknowledgement: {error}"));
    let error = handle
        .try_acknowledge(acknowledgement, Duration::ZERO)
        .err()
        .unwrap_or_else(|| panic!("zero deadline must reject"));

    assert_eq!(
        error.kind(),
        ShareAcknowledgementAdmissionErrorKind::InvalidDeadline
    );
    let acknowledgement = error.into_acknowledgement();
    assert_eq!(acknowledgement.acquisition_count(), 1);
    drop(acknowledgement);
    finish(owner, group_id);
}

#[test]
fn driver_shutdown_publishes_not_sent_without_stranding_session_ownership() {
    let (mut owner, mut handle, _group_id) = staged_handle();
    let acknowledgement = handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("take: {error}"))
        .unwrap_or_else(|| panic!("batch"))
        .accept_all()
        .unwrap_or_else(|error| panic!("acknowledgement: {error}"));
    let observer = handle
        .try_acknowledge(acknowledgement, Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("admission: {error}"))
        .into_observer();

    owner
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("recovery: {error:?}"));
    let ShareAcknowledgeOutcome::Failed(failure) = observer
        .wait()
        .unwrap_or_else(|error| panic!("observe: {error}"))
    else {
        panic!("shutdown failure");
    };
    assert_eq!(
        failure.delivery_status(),
        ShareAcknowledgeDeliveryStatus::NotSent
    );
    assert!(failure.into_retry().is_none());
    assert_eq!(
        publication_turn(&owner),
        ShareAcknowledgementCompletionTurn::Progress
    );
    owner
        .stop_recv_notifier()
        .unwrap_or_else(|| panic!("receive notifier"))
        .join_off_notifier()
        .unwrap_or_else(|error| panic!("join receive notifier: {error}"));
}

#[test]
fn definitely_unsent_driver_failure_returns_the_exact_public_retry() {
    let (owner, mut handle, group_id) = staged_handle();
    let acknowledgement = handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("take: {error}"))
        .unwrap_or_else(|| panic!("batch"))
        .accept_all()
        .unwrap_or_else(|error| panic!("acknowledgement: {error}"));
    let observer = handle
        .try_acknowledge(acknowledgement, Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("admission: {error}"))
        .into_observer();
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));
    {
        let mut registry = owner.lock_registry_for_test();
        let sessions = registry
            .entry_mut(group_id)
            .and_then(|entry| entry.fetch_mut().sessions_mut())
            .unwrap_or_else(|| panic!("sessions"));
        assert_eq!(
            sessions.turn(&driver, Moment::from_tick(0)),
            Ok(ShareFetchSessionSetTurn::Progress)
        );
    }
    driver
        .shutdown_with_turn_limit(32, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown: {error}"));
    {
        let mut registry = owner.lock_registry_for_test();
        let sessions = registry
            .entry_mut(group_id)
            .and_then(|entry| entry.fetch_mut().sessions_mut())
            .unwrap_or_else(|| panic!("sessions"));
        assert_eq!(
            sessions.turn(&driver, Moment::from_tick(1)),
            Ok(ShareFetchSessionSetTurn::Progress)
        );
        assert_eq!(
            sessions.turn(&driver, Moment::from_tick(1)),
            Ok(ShareFetchSessionSetTurn::Progress)
        );
    }
    assert_eq!(
        publication_turn(&owner),
        ShareAcknowledgementCompletionTurn::Progress
    );
    let ShareAcknowledgeOutcome::Failed(failure) = observer
        .wait()
        .unwrap_or_else(|error| panic!("observe: {error}"))
    else {
        panic!("driver failure");
    };
    assert_eq!(
        failure.delivery_status(),
        ShareAcknowledgeDeliveryStatus::NotSent
    );
    let retry = failure
        .into_retry()
        .unwrap_or_else(|| panic!("not-sent terminal retains retry"));
    assert_eq!(retry.acquisition_count(), 1);
    drop(retry);
    assert_eq!(
        publication_turn(&owner),
        ShareAcknowledgementCompletionTurn::Progress
    );
    finish(owner, group_id);
}

#[test]
fn dropping_the_observer_does_not_cancel_the_accepted_acknowledgement() {
    let (owner, mut handle, group_id) = staged_handle();
    let acknowledgement = handle
        .try_take_batch()
        .unwrap_or_else(|error| panic!("take: {error}"))
        .unwrap_or_else(|| panic!("batch"))
        .accept_all()
        .unwrap_or_else(|error| panic!("acknowledgement: {error}"));
    let observer = handle
        .try_acknowledge(acknowledgement, Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("admission: {error}"))
        .into_observer();
    drop(observer);

    settle_success(&owner, group_id);
    assert_eq!(
        publication_turn(&owner),
        ShareAcknowledgementCompletionTurn::Progress
    );
    owner
        .stop_acknowledgement_notifier()
        .unwrap_or_else(|error| panic!("stop acknowledgement notifier: {error:?}"))
        .join_off_notifier()
        .unwrap_or_else(|error| panic!("join acknowledgement notifier: {error}"));
    assert_eq!(
        publication_turn(&owner),
        ShareAcknowledgementCompletionTurn::Progress
    );
    finish(owner, group_id);
}

fn settle_success(
    owner: &super::shard::ShareConsumerShardOwner,
    group_id: kafka_client_core::GroupId,
) {
    let mut registry = owner.lock_registry_for_test();
    let sessions = registry
        .entry_mut(group_id)
        .and_then(|entry| entry.fetch_mut().sessions_mut())
        .unwrap_or_else(|| panic!("session"));
    let session = first_session_mut_for_test(sessions);
    let prepared = session
        .prepared_acknowledgement
        .take()
        .unwrap_or_else(|| panic!("prepared acknowledgement"));
    drop(prepared.request);
    session.acknowledgement_terminal = Some(ShareAcknowledgementTerminal {
        attempt: prepared.attempt,
        acknowledgement: prepared.acknowledgement,
        resolution: ShareAcknowledgeResolution::Succeeded(ShareAcknowledgeSuccess {
            throttle_time_ms: 7,
            outcomes: vec![ShareAcknowledgePartitionOutcome {
                topic_id: [7; 16],
                partition: 0,
                error_code: None,
                error_message: None,
                current_leader: None,
            }],
            endpoints: Vec::new(),
        }),
        route: ShareAcknowledgeRoute::without_token_for_test(prepared.attempt.fence().broker_id()),
    });
    let outcome = session
        .settle_acknowledgement_terminal()
        .unwrap_or_else(|error| panic!("settlement: {error:?}"));
    session
        .retain_settled_acknowledgement(outcome)
        .unwrap_or_else(|_outcome| panic!("retained outcome"));
}

fn publication_turn(
    owner: &super::shard::ShareConsumerShardOwner,
) -> ShareAcknowledgementCompletionTurn {
    owner
        .lock_registry_for_test()
        .turn_one_acknowledgement_completion()
        .unwrap_or_else(|error| panic!("publication: {error:?}"))
}
