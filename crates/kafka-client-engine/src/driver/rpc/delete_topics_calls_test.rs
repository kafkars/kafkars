//! Bounded tracked `DeleteTopics` call-capacity and route-refresh scenarios.

use kafka_client_core::DeleteTopicsInput;
use kafka_wire::{DeleteTopicsResponse, delete_topics_response::DeletableTopicResult};

use super::delete_topics_calls::TrackedDeleteTopicsCalls;
use super::delete_topics_refresh::{
    DeleteTopicsControllerRefreshPoll, SettledDeleteTopicsCall,
    normalized_response_requires_controller_refresh, response_requires_controller_refresh,
};

#[test]
fn tracked_call_capacity_is_reserved_before_submission_handoff() {
    let mut calls = TrackedDeleteTopicsCalls::new(1);
    assert_eq!(calls.retained_count(), 0);
    assert!(calls.try_reserve().is_some());
    assert_eq!(calls.retained_count(), 0);
}

#[test]
fn settled_input_moves_once_while_route_authority_remains_owned() {
    let mut settled =
        SettledDeleteTopicsCall::from_input_for_test(DeleteTopicsInput::InvalidResponse);
    assert_eq!(
        settled.take_input(),
        Some(DeleteTopicsInput::InvalidResponse)
    );
    assert_eq!(settled.take_input(), None);
}

#[test]
fn settled_no_refresh_is_ready_without_driver_owner() {
    let mut settled =
        SettledDeleteTopicsCall::from_input_for_test(DeleteTopicsInput::BrokerResponded {
            outcomes: Vec::new(),
        });

    assert_eq!(
        settled.poll_controller_refresh(None),
        DeleteTopicsControllerRefreshPoll::Ready
    );
}

#[test]
fn only_exact_not_controller_responses_request_route_refresh() {
    assert!(response_requires_controller_refresh(&broker_response(41)));
    assert!(!response_requires_controller_refresh(&broker_response(42)));
    assert!(!response_requires_controller_refresh(&broker_response(0)));
}

#[test]
fn malformed_responses_cannot_retain_controller_refresh_authority() {
    assert!(!normalized_response_requires_controller_refresh(
        &DeleteTopicsInput::InvalidResponse,
        true,
    ));
    assert!(normalized_response_requires_controller_refresh(
        &DeleteTopicsInput::BrokerResponded {
            outcomes: Vec::new(),
        },
        true,
    ));
}

fn broker_response(code: i16) -> Result<DeleteTopicsResponse, kafka_driver::RequestError> {
    let mut topic = DeletableTopicResult::default();
    topic.name = Some("orders".into());
    topic.error_code = code;
    let mut response = DeleteTopicsResponse::default();
    response.responses = vec![topic];
    Ok(response)
}
