//! Bounded tracked `CreatePartitions` call-capacity and route-refresh scenarios.

use kafka_client_core::CreatePartitionsInput;
use kafka_wire::{
    CreatePartitionsResponse, create_partitions_response::CreatePartitionsTopicResult,
};

use super::create_partitions_calls::TrackedCreatePartitionsCalls;
use super::create_partitions_refresh::{
    CreatePartitionsControllerRefreshPoll, SettledCreatePartitionsCall,
    response_requires_controller_refresh,
};

#[test]
fn tracked_call_capacity_is_reserved_before_submission_handoff() {
    let mut calls = TrackedCreatePartitionsCalls::new(1);
    assert_eq!(calls.retained_count(), 0);
    assert!(calls.try_reserve().is_some());
    assert_eq!(calls.retained_count(), 0);
}

#[test]
fn settled_input_moves_once_while_route_authority_remains_owned() {
    let mut settled =
        SettledCreatePartitionsCall::from_input_for_test(CreatePartitionsInput::InvalidResponse);
    assert_eq!(
        settled.take_input(),
        Some(CreatePartitionsInput::InvalidResponse)
    );
    assert_eq!(settled.take_input(), None);
}

#[test]
fn settled_no_refresh_is_ready_without_driver_owner() {
    let mut settled =
        SettledCreatePartitionsCall::from_input_for_test(CreatePartitionsInput::BrokerResponded {
            outcomes: Vec::new(),
        });

    assert_eq!(
        settled.poll_controller_refresh(None),
        CreatePartitionsControllerRefreshPoll::Ready
    );
}

#[test]
fn only_exact_not_controller_responses_request_route_refresh() {
    assert!(response_requires_controller_refresh(&broker_response(41)));
    assert!(!response_requires_controller_refresh(&broker_response(42)));
    assert!(!response_requires_controller_refresh(&broker_response(0)));
}

fn broker_response(code: i16) -> Result<CreatePartitionsResponse, kafka_driver::RequestError> {
    let mut topic = CreatePartitionsTopicResult::default();
    topic.name = "orders".into();
    topic.error_code = code;
    let mut response = CreatePartitionsResponse::default();
    response.results = vec![topic];
    Ok(response)
}
