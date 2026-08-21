//! Typed `ShareGroup` description and member value tests.

use super::{
    ShareGroupAssignment, ShareGroupDescription, ShareGroupMember, ShareGroupTopicPartitions,
};

#[test]
fn description_preserves_epochs_members_assignment_and_authorization_bits() {
    let member = ShareGroupMember::new(
        "member-b".to_owned(),
        Some("rack-a".to_owned()),
        9,
        "client-a".to_owned(),
        "/127.0.0.1".to_owned(),
        vec!["audit".to_owned(), "orders".to_owned()],
        ShareGroupAssignment::new(vec![ShareGroupTopicPartitions::new(
            [3; 16],
            "orders".to_owned(),
            vec![0, 2],
        )]),
    );
    let description = ShareGroupDescription::new(
        "share-workers".to_owned(),
        "Stable".to_owned(),
        11,
        13,
        "uniform".to_owned(),
        vec![member],
        Some(0x21),
    );

    assert_eq!(description.group_id(), "share-workers");
    assert_eq!(description.state(), "Stable");
    assert_eq!(description.group_epoch(), 11);
    assert_eq!(description.assignment_epoch(), 13);
    assert_eq!(description.assignor_name(), "uniform");
    assert_eq!(description.authorized_operations(), Some(0x21));
    let member = &description.members()[0];
    assert_eq!(member.member_id(), "member-b");
    assert_eq!(member.rack_id(), Some("rack-a"));
    assert_eq!(member.member_epoch(), 9);
    assert_eq!(member.client_id(), "client-a");
    assert_eq!(member.client_host(), "/127.0.0.1");
    assert_eq!(
        member
            .subscribed_topic_names()
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["audit", "orders"],
    );
    assert_eq!(member.assignment().topics()[0].topic_name(), "orders");
}
