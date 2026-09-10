//! Real driver-wire evidence for delayed visibility after a successful creation.

mod broker;

use std::time::{Duration, Instant};

use kafka_client_core::{
    CreateTopicResult, CreateTopicSpecification, CreateTopicsInput, CreateTopicsPlan, Deadline,
    Moment, OperationId,
};

use crate::{EngineConfig, clock::OperationDeadline, driver::DriverOwner};

use self::broker::LoopbackBroker;
use super::{super::create_topics_calls::TrackedCreateTopicsCalls, SettledCreateTopicsCall};

#[test]
fn unknown_topic_after_create_waits_then_retries_until_causal_visibility() {
    let mut broker = LoopbackBroker::bind();
    let config = EngineConfig::new(vec![broker.address()])
        .with_client_id(Some("create-topics-visibility-loopback".to_owned()));
    let mut driver =
        DriverOwner::build(&config).unwrap_or_else(|error| panic!("driver owner: {error}"));
    broker.initialize(&mut driver);

    let deadline = OperationDeadline::from_parts_for_test(
        Deadline::from_tick(5_000_000_000),
        Instant::now() + Duration::from_secs(30),
    );
    let operation_id = OperationId::from_raw(19);
    let mut calls = TrackedCreateTopicsCalls::new(1);
    calls
        .try_reserve()
        .unwrap_or_else(|| panic!("CreateTopics capacity must be available"))
        .submit(
            &driver,
            operation_id,
            deadline,
            visibility_plan(),
            64 * 1024,
            Moment::from_tick(1),
        )
        .unwrap_or_else(|error| panic!("submit CreateTopics: {error:?}"));

    broker.respond_create_topics(&mut driver);
    let settled = poll_ready(&mut calls, &mut driver, "settle CreateTopics response");
    let input = settled
        .take_input()
        .unwrap_or_else(|| panic!("broker response must be retained"));
    let CreateTopicsInput::BrokerResponded { outcomes } = &input else {
        panic!("successful loopback response must retain topic outcomes");
    };
    assert!(matches!(outcomes[0].result(), CreateTopicResult::Created));
    assert!(matches!(
        outcomes[1].result(),
        CreateTopicResult::Failed(error) if error.code() == 36
    ));
    assert!(settled.begin_visibility(&driver, operation_id, deadline, Moment::from_tick(1)));

    broker.respond_unknown_topic(&mut driver);
    let lag_observed_at = Moment::from_tick(2);
    assert!(calls.advance_one_visibility(&driver, lag_observed_at));
    assert!(
        calls
            .poll_next_ready()
            .unwrap_or_else(|error| panic!("poll lagging metadata: {error}"))
            .is_none()
    );
    let retry_at = calls
        .next_deadline()
        .unwrap_or_else(|| panic!("lagging metadata must schedule a retry"));
    assert_eq!(retry_at, Deadline::from_tick(25_000_002));
    assert!(!calls.advance_one_visibility(&driver, Moment::from_tick(retry_at.tick() - 1)));
    broker.assert_no_frame_before_retry();

    assert!(calls.advance_one_visibility(&driver, Moment::from_tick(retry_at.tick())));
    broker.respond_visible_topic(&mut driver);
    assert!(calls.advance_one_visibility(&driver, Moment::from_tick(retry_at.tick() + 1)));
    let settled = calls
        .poll_next_ready()
        .unwrap_or_else(|error| panic!("confirm created-topic visibility: {error}"))
        .unwrap_or_else(|| panic!("created-topic visibility must be ready"));
    assert_eq!(
        settled.take_input(),
        Some(CreateTopicsInput::VisibilityConfirmed)
    );
    assert!(calls.discard_settled(operation_id));
    assert_eq!(calls.retained_count(), 0);
}

#[test]
fn recreated_topic_with_reset_leader_epoch_is_confirmed_by_direct_retry() {
    let mut broker = LoopbackBroker::bind();
    let config = EngineConfig::new(vec![broker.address()])
        .with_client_id(Some("create-topics-recreated-topic-loopback".to_owned()));
    let mut driver =
        DriverOwner::build(&config).unwrap_or_else(|error| panic!("driver owner: {error}"));
    broker.initialize(&mut driver);

    let deadline = OperationDeadline::from_parts_for_test(
        Deadline::from_tick(5_000_000_000),
        Instant::now() + Duration::from_secs(30),
    );
    let operation_id = OperationId::from_raw(23);
    let mut calls = TrackedCreateTopicsCalls::new(1);
    calls
        .try_reserve()
        .unwrap_or_else(|| panic!("CreateTopics capacity must be available"))
        .submit(
            &driver,
            operation_id,
            deadline,
            visibility_plan(),
            64 * 1024,
            Moment::from_tick(1),
        )
        .unwrap_or_else(|error| panic!("submit CreateTopics: {error:?}"));

    broker.respond_create_topics(&mut driver);
    let settled = poll_ready(&mut calls, &mut driver, "settle CreateTopics response");
    assert!(settled.begin_visibility(&driver, operation_id, deadline, Moment::from_tick(1)));

    // This is the deleted topic still visible with its old ID and high epoch.
    broker.respond_stale_topic(&mut driver);
    let stale_observed_at = Moment::from_tick(2);
    assert!(calls.advance_one_visibility(&driver, stale_observed_at));
    let retry_at = calls
        .next_deadline()
        .unwrap_or_else(|| panic!("stale topology must schedule a retry"));

    assert!(calls.advance_one_visibility(&driver, Moment::from_tick(retry_at.tick())));
    // The replacement has a new topic ID and reset epoch. Feeding this through
    // the driver's old name/partition cache fence would reject it as regression.
    broker.respond_visible_topic(&mut driver);
    assert!(calls.advance_one_visibility(&driver, Moment::from_tick(retry_at.tick() + 1)));
    let settled = calls
        .poll_next_ready()
        .unwrap_or_else(|error| panic!("poll recreated-topic metadata: {error}"))
        .unwrap_or_else(|| panic!("recreated-topic visibility must be ready"));
    assert_eq!(
        settled.take_input(),
        Some(CreateTopicsInput::VisibilityConfirmed)
    );
}

fn visibility_plan() -> CreateTopicsPlan {
    CreateTopicsPlan::new(
        vec![
            CreateTopicSpecification::new("fresh", 2, 1, Vec::new()),
            CreateTopicSpecification::new("existing", 1, 1, Vec::new()),
        ],
        false,
    )
    .unwrap_or_else(|error| panic!("valid visibility plan: {error}"))
}

fn poll_ready<'a>(
    calls: &'a mut TrackedCreateTopicsCalls,
    driver: &mut DriverOwner,
    phase: &str,
) -> &'a mut SettledCreateTopicsCall {
    for _turn in 0..32 {
        driver
            .turn(Duration::from_millis(100))
            .unwrap_or_else(|error| panic!("{phase}: {error}"));
        if calls
            .poll_next_ready()
            .unwrap_or_else(|error| panic!("{phase}: {error}"))
            .is_some()
        {
            return calls
                .poll_next_ready()
                .unwrap_or_else(|error| panic!("{phase}: {error}"))
                .unwrap_or_else(|| panic!("{phase} lost its settled call"));
        }
    }
    panic!("{phase} did not settle in bounded turns")
}
