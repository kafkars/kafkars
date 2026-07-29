//! Stable-version correlation, topology-description semantics, ordering, and bounds.

use kafka_wire::{
    StreamsGroupDescribeResponse,
    streams_group_describe_response::{
        DescribedGroup, Member, TaskIds, TopologyDescription, TopologyDescriptionNode,
        TopologyDescriptionSubtopology,
    },
};

use super::{
    DescribeStreamsGroupProtocolFailure, NormalizedDescribeStreamsGroupResult,
    normalize_describe_streams_group_response_with_charge,
    response::normalize_describe_streams_group_response,
};

const RESULT_LIMIT: usize = 4 * 1024 * 1024;

#[test]
fn v0_success_is_singleton_correlated_and_canonicalized() {
    let mut response = StreamsGroupDescribeResponse::default();
    response.throttle_time_ms = 17;
    let mut group = success_group();
    group.members = vec![
        member("zeta", vec![task_ids("b", vec![2, 0])]),
        member("alpha", vec![]),
    ];
    response.groups = vec![group];

    let normalized = normalize_describe_streams_group_response(
        "streams-app",
        false,
        false,
        Some(0),
        &response,
        RESULT_LIMIT,
    )
    .unwrap_or_else(|error| panic!("valid response: {error:?}"));
    let NormalizedDescribeStreamsGroupResult::Described(result) = normalized else {
        panic!("description expected");
    };
    assert_eq!(result.throttle_time_ms(), 17);
    assert_eq!(result.description().group_id(), "streams-app");
    assert_eq!(result.description().members()[0].member_id(), "alpha");
    assert!(result.description().topology_description_status().is_none());
    let (_, description) = result.into_parts();
    let (_, _, _, _, _, members, _, _, _) = description.into_parts();
    let (_, _, _, _, _, _, _, _, _, _, _, _, assignment, _, _) = members
        .into_iter()
        .nth(1)
        .unwrap_or_else(|| panic!("zeta"))
        .into_parts();
    let (active, _, _) = assignment.into_parts();
    assert_eq!(active[0].partitions(), [0, 2]);
}

#[test]
fn v1_topology_description_requires_matching_status_and_preserves_unknown_status() {
    let mut group = success_group();
    group.topology_description_status = 3;
    let mut description = TopologyDescription::default();
    let mut subtopology = TopologyDescriptionSubtopology::default();
    subtopology.subtopology_id = "sub-1".into();
    subtopology.nodes = vec![node("future", 91), node("source", 1)];
    description.subtopologies = vec![subtopology];
    group.topology_description = Some(description);
    let mut response = StreamsGroupDescribeResponse::default();
    response.groups = vec![group];

    let normalized = normalize_describe_streams_group_response(
        "streams-app",
        false,
        true,
        Some(1),
        &response,
        RESULT_LIMIT,
    )
    .unwrap_or_else(|error| panic!("valid topology response: {error:?}"));
    let NormalizedDescribeStreamsGroupResult::Described(result) = normalized else {
        panic!("description expected");
    };
    assert_eq!(
        result
            .description()
            .topology_description_status()
            .unwrap_or_else(|| panic!("v1 status"))
            .raw(),
        3
    );
    let (_, description) = result.into_parts();
    let (_, _, _, _, _, _, _, topology_description, _) = description.into_parts();
    let (subtopologies, _) = topology_description
        .unwrap_or_else(|| panic!("available topology description"))
        .into_parts();
    let (_, nodes) = subtopologies
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("subtopology"))
        .into_parts();
    let (_, node_type, _, _, _, _) = nodes
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("future node"))
        .into_parts();
    assert_eq!(node_type, 91);

    let mut unknown = success_group();
    unknown.topology_description_status = 91;
    let mut response = StreamsGroupDescribeResponse::default();
    response.groups = vec![unknown];
    let normalized = normalize_describe_streams_group_response(
        "streams-app",
        false,
        true,
        Some(1),
        &response,
        RESULT_LIMIT,
    )
    .unwrap_or_else(|error| panic!("unknown status is exact: {error:?}"));
    let NormalizedDescribeStreamsGroupResult::Described(result) = normalized else {
        panic!("description expected");
    };
    assert_eq!(
        result
            .description()
            .topology_description_status()
            .unwrap_or_else(|| panic!("v1 status"))
            .raw(),
        91
    );
}

#[test]
fn invalid_version_status_correlation_and_duplicates_are_rejected() {
    let mut response = StreamsGroupDescribeResponse::default();
    response.groups = vec![success_group()];
    assert_eq!(
        normalize_describe_streams_group_response(
            "streams-app",
            false,
            true,
            Some(0),
            &response,
            RESULT_LIMIT,
        ),
        Err(DescribeStreamsGroupProtocolFailure::TopologyDescriptionRequiresV1)
    );

    response.groups[0].topology_description_status = 3;
    assert_eq!(
        normalize_describe_streams_group_response(
            "streams-app",
            false,
            true,
            Some(1),
            &response,
            RESULT_LIMIT,
        ),
        Err(DescribeStreamsGroupProtocolFailure::TopologyDescriptionStatusMismatch)
    );

    let mut duplicate = success_group();
    duplicate.members = vec![member("same", vec![]), member("same", vec![])];
    response.groups = vec![duplicate];
    assert_eq!(
        normalize_describe_streams_group_response(
            "streams-app",
            false,
            false,
            Some(0),
            &response,
            RESULT_LIMIT,
        ),
        Err(DescribeStreamsGroupProtocolFailure::DuplicateIdentity)
    );
}

#[test]
fn broker_error_is_exact_and_large_success_is_rejected_before_copying() {
    let mut rejected = DescribedGroup::default();
    rejected.group_id = "streams-app".into();
    rejected.error_code = -32_000;
    rejected.error_message = Some("denied".into());
    let mut response = StreamsGroupDescribeResponse::default();
    response.throttle_time_ms = 9;
    response.groups = vec![rejected];
    let normalized = normalize_describe_streams_group_response(
        "streams-app",
        false,
        false,
        Some(0),
        &response,
        RESULT_LIMIT,
    )
    .unwrap_or_else(|error| panic!("broker error: {error:?}"));
    let NormalizedDescribeStreamsGroupResult::Failed(error) = normalized else {
        panic!("broker error expected");
    };
    assert_eq!(
        error.into_parts(),
        (9, -32_000, Some("denied".to_owned()), false)
    );

    response.groups = vec![success_group()];
    assert!(matches!(
        normalize_describe_streams_group_response(
            "streams-app",
            false,
            false,
            Some(0),
            &response,
            1,
        ),
        Err(DescribeStreamsGroupProtocolFailure::RetainedBytes { .. })
    ));
}

#[test]
fn charged_adapter_preserves_exact_terminal_size_and_rejects_over_budget_results() {
    let mut response = StreamsGroupDescribeResponse::default();
    response.groups = vec![success_group()];

    let (normalized, retained_bytes) = normalize_describe_streams_group_response_with_charge(
        "streams-app",
        false,
        false,
        Some(0),
        &response,
        RESULT_LIMIT,
    )
    .unwrap_or_else(|error| panic!("charged success: {error:?}"));
    assert!(matches!(
        normalized,
        NormalizedDescribeStreamsGroupResult::Described(_)
    ));
    assert_eq!(
        normalize_describe_streams_group_response_with_charge(
            "streams-app",
            false,
            false,
            Some(0),
            &response,
            retained_bytes - 1,
        ),
        Err(DescribeStreamsGroupProtocolFailure::RetainedBytes {
            required: retained_bytes,
            limit: retained_bytes - 1,
        })
    );

    let mut rejected = DescribedGroup::default();
    rejected.group_id = "streams-app".into();
    rejected.error_code = -32_000;
    rejected.error_message = Some("denied".into());
    response.groups = vec![rejected];
    let (_, rejected_bytes) = normalize_describe_streams_group_response_with_charge(
        "streams-app",
        false,
        false,
        Some(0),
        &response,
        RESULT_LIMIT,
    )
    .unwrap_or_else(|error| panic!("charged broker error: {error:?}"));
    assert_eq!(rejected_bytes, "streams-app".len() + "denied".len());
}

fn success_group() -> DescribedGroup {
    let mut group = DescribedGroup::default();
    group.group_id = "streams-app".into();
    group.group_state = "Stable".into();
    group.group_epoch = 8;
    group.assignment_epoch = 7;
    group.authorized_operations = i32::MIN;
    group
}

fn member(member_id: &str, active_tasks: Vec<TaskIds>) -> Member {
    let mut member = Member::default();
    member.member_id = member_id.into();
    member.member_epoch = 4;
    member.topology_epoch = 2;
    member.process_id = "process-a".into();
    member.assignment.active_tasks = active_tasks;
    member
}

fn task_ids(subtopology_id: &str, partitions: Vec<i32>) -> TaskIds {
    let mut tasks = TaskIds::default();
    tasks.subtopology_id = subtopology_id.into();
    tasks.partitions = partitions;
    tasks
}

fn node(name: &str, node_type: i8) -> TopologyDescriptionNode {
    let mut node = TopologyDescriptionNode::default();
    node.name = name.into();
    node.node_type = node_type;
    node
}
