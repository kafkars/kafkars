//! Linear settlement and causal-visibility targets for tracked `CreateTopics` calls.

use core::num::NonZeroI16;

use kafka_client_core::{
    CreateTopicBrokerError, CreateTopicOutcome, CreateTopicReplicaAssignment,
    CreateTopicSpecification, CreateTopicsInput, CreateTopicsPlan, OperationId,
};

use super::{
    create_topics_calls::TrackedCreateTopicsCalls,
    create_topics_visibility::{SettledCreateTopicsCall, visibility_targets},
};

#[test]
fn settled_input_moves_once_while_route_authority_remains_owned() {
    let mut settled = SettledCreateTopicsCall::from_input_for_test(
        OperationId::from_raw(1),
        CreateTopicsInput::InvalidResponse,
    );
    assert_eq!(
        settled.take_input(),
        Some(CreateTopicsInput::InvalidResponse)
    );
    assert_eq!(settled.take_input(), None);
}

#[test]
fn pending_visibility_does_not_block_another_ready_settlement() {
    let mut calls = TrackedCreateTopicsCalls::new(2);
    let mut pending = SettledCreateTopicsCall::from_input_for_test(
        OperationId::from_raw(1),
        CreateTopicsInput::VisibilityConfirmed,
    );
    assert_eq!(
        pending.take_input(),
        Some(CreateTopicsInput::VisibilityConfirmed)
    );
    calls.retain_settled_for_test(pending);
    calls.retain_settled_for_test(SettledCreateTopicsCall::from_input_for_test(
        OperationId::from_raw(2),
        CreateTopicsInput::InvalidResponse,
    ));

    assert_eq!(
        calls.take_ready_input_for_test(),
        Some((OperationId::from_raw(2), CreateTopicsInput::InvalidResponse))
    );
}

#[test]
fn only_successful_topics_require_exact_requested_visibility() {
    let plan = CreateTopicsPlan::new(
        vec![
            CreateTopicSpecification::new("fresh", 3, 1, Vec::new()),
            CreateTopicSpecification::new("existing", 2, 1, Vec::new()),
        ],
        false,
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"));
    let code = NonZeroI16::new(36).unwrap_or_else(|| panic!("nonzero broker code"));
    let input = CreateTopicsInput::BrokerResponded {
        outcomes: vec![
            CreateTopicOutcome::created("fresh"),
            CreateTopicOutcome::failed("existing", CreateTopicBrokerError::new(code, None)),
        ],
    };

    let targets =
        visibility_targets(&plan, &input).unwrap_or_else(|()| panic!("valid visibility targets"));
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].topic_for_test(), "fresh");
    assert_eq!(targets[0].partition_count_for_test(), 3);
}

#[test]
fn manual_placement_uses_assignment_count() {
    let plan = CreateTopicsPlan::new(
        vec![CreateTopicSpecification::manual(
            "manual",
            vec![
                CreateTopicReplicaAssignment::new(0, vec![1]),
                CreateTopicReplicaAssignment::new(1, vec![1]),
            ],
            None,
            Vec::new(),
        )],
        false,
    )
    .unwrap_or_else(|error| panic!("valid manual plan: {error}"));
    let input = CreateTopicsInput::BrokerResponded {
        outcomes: vec![CreateTopicOutcome::created("manual")],
    };

    let targets =
        visibility_targets(&plan, &input).unwrap_or_else(|()| panic!("valid visibility targets"));
    assert_eq!(targets[0].partition_count_for_test(), 2);
}

#[test]
fn validate_only_response_never_requests_visibility() {
    let plan = CreateTopicsPlan::new(
        vec![CreateTopicSpecification::new("fresh", 3, 1, Vec::new())],
        true,
    )
    .unwrap_or_else(|error| panic!("valid validate-only plan: {error}"));
    let input = CreateTopicsInput::BrokerResponded {
        outcomes: vec![CreateTopicOutcome::created("fresh")],
    };

    assert!(
        visibility_targets(&plan, &input)
            .unwrap_or_else(|()| panic!("valid visibility targets"))
            .is_empty()
    );
}
