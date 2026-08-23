//! Driver handoff, exact success, and conservative shutdown settlement evidence.

use std::time::Duration;

use kafka_client_core::{DeliveryStatus, ShareFetchSessionPhase};

use crate::{
    EngineConfig,
    clock::MonotonicClock,
    driver::{DriverOwner, ShareAcknowledgeResolution, ShareAcknowledgeRoute},
    protocol::consumer::share_acknowledge::{
        ShareAcknowledgePartitionOutcome, ShareAcknowledgeSuccess,
    },
};

use super::{
    super::{
        fetch_acknowledgement::ShareAcknowledgementTerminal,
        fetch_acknowledgement_test::delivered_acknowledgement,
        fetch_session::ShareFetchSessionOwner,
    },
    ShareAcknowledgementExecutionFailureKind, ShareAcknowledgementExecutionOutcome,
    ShareAcknowledgementExecutionPoll, ShareAcknowledgementSubmissionTurn,
};

#[test]
fn all_success_response_advances_session_and_retires_exact_acquisitions() {
    let (mut owner, acknowledgement) = delivered_acknowledgement();
    prepare(&mut owner, acknowledgement);
    let prepared = owner
        .prepared_acknowledgement
        .take()
        .unwrap_or_else(|| panic!("prepared acknowledgement"));
    drop(prepared.request);
    owner.acknowledgement_terminal = Some(ShareAcknowledgementTerminal {
        attempt: prepared.attempt,
        acknowledgement: prepared.acknowledgement,
        resolution: ShareAcknowledgeResolution::Succeeded(ShareAcknowledgeSuccess {
            throttle_time_ms: 0,
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

    let outcome = owner
        .settle_acknowledgement_terminal()
        .unwrap_or_else(|error| panic!("ack settlement: {error:?}"));

    assert!(matches!(
        outcome,
        ShareAcknowledgementExecutionOutcome::Responded(_)
    ));
    assert_eq!(owner.machine().phase(), ShareFetchSessionPhase::Ready);
    assert_eq!(owner.machine().fence().session_epoch().get(), 2);
    assert!(owner.machine().ledger().is_empty());
}

#[test]
fn accepted_driver_shutdown_preserves_not_sent_retry_ownership() {
    let mut driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));
    let (mut owner, acknowledgement) = delivered_acknowledgement();
    prepare(&mut owner, acknowledgement);
    assert_eq!(
        owner.submit_prepared_acknowledgement(&driver, kafka_client_core::Moment::from_tick(0)),
        Ok(ShareAcknowledgementSubmissionTurn::Submitted)
    );
    driver
        .shutdown_with_turn_limit(32, Duration::from_millis(10))
        .unwrap_or_else(|error| panic!("shutdown: {error}"));
    assert_eq!(
        owner.poll_acknowledgement(),
        Ok(ShareAcknowledgementExecutionPoll::Terminal)
    );
    let outcome = owner
        .settle_acknowledgement_terminal()
        .unwrap_or_else(|error| panic!("settlement: {error:?}"));
    let ShareAcknowledgementExecutionOutcome::Failed {
        kind: ShareAcknowledgementExecutionFailureKind::Driver(_),
        delivery: DeliveryStatus::NotSent,
        retry: Some(retry),
    } = outcome
    else {
        panic!("definitely-unsent retry expected");
    };
    assert_eq!(retry.acquisitions().len(), 1);
    assert_eq!(owner.machine().phase(), ShareFetchSessionPhase::Ready);
}

#[test]
fn unique_driver_teardown_retires_ambiguous_acquisitions() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));
    let (mut owner, acknowledgement) = delivered_acknowledgement();
    prepare(&mut owner, acknowledgement);
    owner
        .submit_prepared_acknowledgement(&driver, kafka_client_core::Moment::from_tick(0))
        .unwrap_or_else(|error| panic!("submit: {error:?}"));
    drop(driver);

    assert_eq!(
        owner.recover_acknowledgement_after_driver_shutdown(),
        Ok(true)
    );
    let outcome = owner
        .take_acknowledgement_outcome()
        .unwrap_or_else(|| panic!("recovery outcome"));
    assert!(matches!(
        outcome,
        ShareAcknowledgementExecutionOutcome::Failed {
            delivery: DeliveryStatus::PossiblySent,
            retry: None,
            ..
        }
    ));
    assert_eq!(owner.machine().phase(), ShareFetchSessionPhase::Lost);
    assert!(owner.machine().ledger().is_empty());
}

fn prepare(
    owner: &mut ShareFetchSessionOwner,
    acknowledgement: kafka_client_core::ShareAcknowledgement,
) {
    let clock = MonotonicClock::new();
    let capture = clock
        .capture_deadline_after(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("deadline: {error:?}"));
    owner
        .prepare_acknowledgement(acknowledgement, capture, capture.now())
        .unwrap_or_else(|failure| panic!("preparation: {:?}", failure.kind));
}
