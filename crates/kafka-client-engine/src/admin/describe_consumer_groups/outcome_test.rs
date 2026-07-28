//! Lossless core-to-engine group-description translation scenarios.

use kafka_client_core::{
    AdminClassicConsumerGroupDetails, AdminClassicConsumerGroupMemberDetails,
    AdminConsumerGroupDescription, AdminConsumerGroupDescriptionDetails,
    AdminConsumerGroupDescriptionMember, AdminConsumerGroupDescriptionOutcome,
    AdminConsumerGroupMemberDetails, AdminDescribeConsumerGroupsBatch,
    AdminDescribeConsumerGroupsTerminal,
};

use super::{
    ConsumerGroupDescriptionDetails, ConsumerGroupMemberDetails, DescribeConsumerGroupsOutcome,
    outcome::translate_terminal,
};

#[test]
fn scalar_raw_member_and_authorization_facts_cross_losslessly() {
    let terminal =
        AdminDescribeConsumerGroupsTerminal::Described(AdminDescribeConsumerGroupsBatch::new(
            17,
            vec![AdminConsumerGroupDescriptionOutcome::described(
                "workers".to_owned(),
                AdminConsumerGroupDescription::new(
                    "Stable".to_owned(),
                    AdminConsumerGroupDescriptionDetails::Classic(
                        AdminClassicConsumerGroupDetails::new(
                            "consumer".to_owned(),
                            "range".to_owned(),
                        ),
                    ),
                    vec![AdminConsumerGroupDescriptionMember::new(
                        "member".to_owned(),
                        Some("instance".to_owned()),
                        "client".to_owned(),
                        "host".to_owned(),
                        AdminConsumerGroupMemberDetails::Classic(
                            AdminClassicConsumerGroupMemberDetails::new(vec![1], vec![2]),
                        ),
                    )],
                    Some(3),
                ),
            )],
        ));
    let DescribeConsumerGroupsOutcome::Groups(batch) = translate_terminal(terminal) else {
        panic!("success became failure");
    };
    let (throttle, groups) = batch.into_parts();
    assert_eq!(throttle, 17);
    let Some(group) = groups.into_iter().next() else {
        panic!("group expected");
    };
    let (group_id, description) = group.into_parts();
    assert_eq!(group_id, "workers");
    let Ok(description) = description else {
        panic!("successful description expected");
    };
    let (state, details, members, operations) = description.into_parts();
    let ConsumerGroupDescriptionDetails::Classic(details) = details else {
        panic!("classic facts changed protocol kind");
    };
    let (protocol_type, protocol_data) = details.into_parts();
    assert_eq!(
        (
            state.as_str(),
            protocol_type.as_str(),
            protocol_data.as_str()
        ),
        ("Stable", "consumer", "range")
    );
    assert_eq!(operations, Some(3));
    assert_eq!(members.len(), 1);
    let Some(member) = members.into_iter().next() else {
        panic!("member expected");
    };
    let (_, _, _, _, details) = member.into_parts();
    let ConsumerGroupMemberDetails::Classic(details) = details else {
        panic!("classic member facts changed protocol kind");
    };
    assert_eq!(details.into_parts(), (vec![1], vec![2]));
}
