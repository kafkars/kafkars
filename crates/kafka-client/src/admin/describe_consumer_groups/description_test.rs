//! Stable public classic and KIP-848 group/member fact tests.

use super::{
    ClassicConsumerGroupDetails, ClassicConsumerGroupMemberDetails, ConsumerGroupAssignment,
    ConsumerGroupDescription, ConsumerGroupDescriptionDetails, ConsumerGroupMember,
    ConsumerGroupMemberDetails, ConsumerGroupTopicPartitions, ConsumerProtocolGroupDetails,
    ConsumerProtocolMemberDetails,
};

#[test]
fn classic_nullable_instance_raw_payloads_and_authorization_bits_remain_exact() {
    let member = ConsumerGroupMember::new(
        "member".to_owned(),
        Some("instance".to_owned()),
        "client".to_owned(),
        "host".to_owned(),
        ConsumerGroupMemberDetails::Classic(ClassicConsumerGroupMemberDetails::new(
            vec![1, 2],
            vec![3, 4],
        )),
    );
    let description = ConsumerGroupDescription::new(
        "Stable".to_owned(),
        ConsumerGroupDescriptionDetails::Classic(ClassicConsumerGroupDetails::new(
            "consumer".to_owned(),
            "range".to_owned(),
        )),
        vec![member],
        Some(0x20),
    );

    assert_eq!(description.state(), "Stable");
    assert_eq!(description.authorized_operations(), Some(0x20));
    let ConsumerGroupDescriptionDetails::Classic(details) = description.details() else {
        panic!("classic description changed protocol variants");
    };
    assert_eq!(details.protocol_type(), "consumer");
    assert_eq!(details.protocol_data(), "range");

    let member = &description.members()[0];
    assert_eq!(member.member_id(), "member");
    assert_eq!(member.group_instance_id(), Some("instance"));
    assert_eq!(member.client_id(), "client");
    assert_eq!(member.client_host(), "host");
    let ConsumerGroupMemberDetails::Classic(details) = member.details() else {
        panic!("classic member changed protocol variants");
    };
    assert_eq!(details.metadata(), [1, 2]);
    assert_eq!(details.assignment(), [3, 4]);
}

#[test]
fn consumer_protocol_typed_subscription_and_assignments_remain_exact() {
    let topic_id = [0x21; 16];
    let current_assignment = ConsumerGroupAssignment::new(vec![ConsumerGroupTopicPartitions::new(
        topic_id,
        "orders".to_owned(),
        vec![0, 2],
    )]);
    let target_assignment = ConsumerGroupAssignment::new(vec![ConsumerGroupTopicPartitions::new(
        topic_id,
        "orders".to_owned(),
        vec![0, 1, 2],
    )]);
    let member = ConsumerGroupMember::new(
        "member".to_owned(),
        Some("instance".to_owned()),
        "client".to_owned(),
        "host".to_owned(),
        ConsumerGroupMemberDetails::Consumer(ConsumerProtocolMemberDetails::new(
            Some("rack-a".to_owned()),
            17,
            vec!["orders".to_owned(), "payments".to_owned()],
            Some("orders-.*".to_owned()),
            current_assignment,
            target_assignment,
            Some(1),
        )),
    );
    let description = ConsumerGroupDescription::new(
        "Stable".to_owned(),
        ConsumerGroupDescriptionDetails::Consumer(ConsumerProtocolGroupDetails::new(
            19,
            23,
            "uniform".to_owned(),
        )),
        vec![member],
        None,
    );

    assert_eq!(description.state(), "Stable");
    assert_eq!(description.authorized_operations(), None);
    let ConsumerGroupDescriptionDetails::Consumer(details) = description.details() else {
        panic!("consumer-protocol description changed protocol variants");
    };
    assert_eq!(details.group_epoch(), 19);
    assert_eq!(details.assignment_epoch(), 23);
    assert_eq!(details.assignor_name(), "uniform");

    let member = &description.members()[0];
    let ConsumerGroupMemberDetails::Consumer(details) = member.details() else {
        panic!("consumer-protocol member changed protocol variants");
    };
    assert_eq!(details.rack_id(), Some("rack-a"));
    assert_eq!(details.member_epoch(), 17);
    assert_eq!(details.subscribed_topic_names(), ["orders", "payments"]);
    assert_eq!(details.subscribed_topic_regex(), Some("orders-.*"));
    assert_eq!(details.member_type(), Some(1));
    let current = &details.assignment().topics()[0];
    assert_eq!(current.topic_id(), topic_id);
    assert_eq!(current.topic_name(), "orders");
    assert_eq!(current.partitions(), [0, 2]);
    let target = &details.target_assignment().topics()[0];
    assert_eq!(target.topic_id(), topic_id);
    assert_eq!(target.topic_name(), "orders");
    assert_eq!(target.partitions(), [0, 1, 2]);
}
