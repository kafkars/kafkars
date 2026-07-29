//! Exhaustive classic-only result translation tests.

use std::time::Duration;

use super::DescribeClassicGroupsResult;
use crate::{
    BatchResult, ErrorKind, KafkaError,
    admin::{
        ClassicConsumerGroupDetails, ClassicConsumerGroupMemberDetails, ConsumerGroupDescription,
        ConsumerGroupDescriptionDetails, ConsumerGroupMember, ConsumerGroupMemberDetails,
        ConsumerProtocolGroupDetails, ConsumerProtocolMemberDetails, DescribeConsumerGroupsResult,
    },
};

#[test]
fn classic_translation_preserves_order_throttle_errors_and_raw_payloads() {
    let classic = ConsumerGroupDescription::new(
        "Stable".to_owned(),
        ConsumerGroupDescriptionDetails::Classic(ClassicConsumerGroupDetails::new(
            "consumer".to_owned(),
            "range".to_owned(),
        )),
        vec![ConsumerGroupMember::new(
            "member-a".to_owned(),
            Some("instance-a".to_owned()),
            "client-a".to_owned(),
            "host-a".to_owned(),
            ConsumerGroupMemberDetails::Classic(ClassicConsumerGroupMemberDetails::new(
                vec![1, 2],
                vec![3, 4],
            )),
        )],
        Some(0x20),
    );
    let broker_error = KafkaError::new(ErrorKind::Broker, "group rejected");
    let source = DescribeConsumerGroupsResult::new(
        Duration::from_millis(47),
        BatchResult::new(vec![
            ("classic".to_owned(), Ok(classic)),
            ("rejected".to_owned(), Err(broker_error.clone())),
        ]),
    );

    let result = DescribeClassicGroupsResult::from_consumer(source);

    assert_eq!(result.throttle_time(), Duration::from_millis(47));
    let entries = result.groups().entries();
    assert_eq!(entries[0].0, "classic");
    let description = entries[0]
        .1
        .as_ref()
        .unwrap_or_else(|error| panic!("classic group: {error}"));
    assert_eq!(description.protocol_type(), "consumer");
    assert_eq!(description.protocol_data(), "range");
    assert_eq!(description.members()[0].metadata(), [1, 2]);
    assert_eq!(description.members()[0].assignment(), [3, 4]);
    assert_eq!(entries[1], ("rejected".to_owned(), Err(broker_error)));
}

#[test]
fn modern_group_or_member_on_classic_only_path_is_an_internal_entry_error() {
    let modern_group = ConsumerGroupDescription::new(
        "Stable".to_owned(),
        ConsumerGroupDescriptionDetails::Consumer(ConsumerProtocolGroupDetails::new(
            1,
            2,
            "uniform".to_owned(),
        )),
        Vec::new(),
        None,
    );
    let modern_member = ConsumerGroupDescription::new(
        "Stable".to_owned(),
        ConsumerGroupDescriptionDetails::Classic(ClassicConsumerGroupDetails::new(
            "consumer".to_owned(),
            "range".to_owned(),
        )),
        vec![ConsumerGroupMember::new(
            "member".to_owned(),
            None,
            "client".to_owned(),
            "host".to_owned(),
            ConsumerGroupMemberDetails::Consumer(ConsumerProtocolMemberDetails::new(
                None,
                1,
                Vec::new(),
                None,
                crate::ConsumerGroupAssignment::new(Vec::new()),
                crate::ConsumerGroupAssignment::new(Vec::new()),
                None,
            )),
        )],
        None,
    );
    let source = DescribeConsumerGroupsResult::new(
        Duration::ZERO,
        BatchResult::new(vec![
            ("modern-group".to_owned(), Ok(modern_group)),
            ("modern-member".to_owned(), Ok(modern_member)),
        ]),
    );

    let result = DescribeClassicGroupsResult::from_consumer(source);

    for (_, outcome) in result.groups().entries() {
        let error = outcome
            .as_ref()
            .err()
            .unwrap_or_else(|| panic!("modern variant must reject"));
        assert_eq!(error.kind(), ErrorKind::Internal);
    }
}
