//! Typed `StreamsGroup` description and member value tests.

use super::{
    StreamsGroupAssignment, StreamsGroupDescription, StreamsGroupEndpoint, StreamsGroupKeyValue,
    StreamsGroupMember, StreamsGroupTaskIds, StreamsGroupTaskOffset, StreamsGroupTopology,
    StreamsGroupTopologyDescription, StreamsGroupTopologyDescriptionStatus,
};

fn assignment(id: &str) -> StreamsGroupAssignment {
    StreamsGroupAssignment::new(
        vec![StreamsGroupTaskIds::new(id.to_owned(), vec![0, 2])],
        Vec::new(),
        Vec::new(),
    )
}

#[test]
fn description_preserves_all_member_and_optional_version_facts() {
    let member = StreamsGroupMember::new(
        "member-b".to_owned(),
        9,
        Some("instance-a".to_owned()),
        Some("rack-a".to_owned()),
        "client-a".to_owned(),
        "/127.0.0.1".to_owned(),
        7,
        "process-a".to_owned(),
        Some(StreamsGroupEndpoint::new("localhost".to_owned(), 8080)),
        vec![StreamsGroupKeyValue::new(
            "rack".to_owned(),
            "az-a".to_owned(),
        )],
        vec![StreamsGroupTaskOffset::new("sub-a".to_owned(), 0, 91)],
        vec![StreamsGroupTaskOffset::new("sub-a".to_owned(), 0, 100)],
        assignment("current"),
        assignment("target"),
        true,
    );
    let description = StreamsGroupDescription::new(
        "streams-workers".to_owned(),
        "Stable".to_owned(),
        11,
        13,
        Some(StreamsGroupTopology::new(5, None)),
        vec![member],
        Some(0x21),
        Some(StreamsGroupTopologyDescription::new(Vec::new(), Vec::new())),
        Some(StreamsGroupTopologyDescriptionStatus::Available),
    );

    assert_eq!(description.group_id(), "streams-workers");
    assert_eq!(description.state(), "Stable");
    assert_eq!(description.group_epoch(), 11);
    assert_eq!(description.assignment_epoch(), 13);
    assert_eq!(
        description.topology().map(StreamsGroupTopology::epoch),
        Some(5)
    );
    assert_eq!(description.authorized_operations(), Some(0x21));
    assert!(description.topology_description().is_some());
    assert_eq!(
        description.topology_description_status(),
        Some(StreamsGroupTopologyDescriptionStatus::Available)
    );

    let member = &description.members()[0];
    assert_eq!(member.member_id(), "member-b");
    assert_eq!(member.member_epoch(), 9);
    assert_eq!(member.instance_id(), Some("instance-a"));
    assert_eq!(member.rack_id(), Some("rack-a"));
    assert_eq!(member.client_id(), "client-a");
    assert_eq!(member.client_host(), "/127.0.0.1");
    assert_eq!(member.topology_epoch(), 7);
    assert_eq!(member.process_id(), "process-a");
    assert_eq!(
        member.user_endpoint().map(StreamsGroupEndpoint::port),
        Some(8080)
    );
    assert_eq!(member.client_tags()[0].value(), "az-a");
    assert_eq!(member.task_offsets()[0].offset(), 91);
    assert_eq!(member.task_end_offsets()[0].offset(), 100);
    assert_eq!(
        member.assignment().active_tasks()[0].subtopology_id(),
        "current"
    );
    assert_eq!(
        member.target_assignment().active_tasks()[0].subtopology_id(),
        "target"
    );
    assert!(member.is_classic());
}

#[test]
fn v0_absence_remains_distinct_from_v1_not_requested() {
    let absent = StreamsGroupDescription::new(
        "streams-workers".to_owned(),
        "Empty".to_owned(),
        1,
        1,
        None,
        Vec::new(),
        None,
        None,
        None,
    );
    let represented = StreamsGroupDescription::new(
        "streams-workers".to_owned(),
        "Empty".to_owned(),
        1,
        1,
        None,
        Vec::new(),
        None,
        None,
        Some(StreamsGroupTopologyDescriptionStatus::NotRequested),
    );

    assert_eq!(absent.topology_description_status(), None);
    assert_eq!(
        represented.topology_description_status(),
        Some(StreamsGroupTopologyDescriptionStatus::NotRequested)
    );
}
