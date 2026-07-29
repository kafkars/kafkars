//! Stable classic group and member value tests.

use super::{ClassicGroupDescription, ClassicGroupMember};

#[test]
fn classic_values_preserve_protocol_payload_identity_and_authorization_bits() {
    let member = ClassicGroupMember::new(
        "member-a".to_owned(),
        Some("instance-a".to_owned()),
        "client-a".to_owned(),
        "/127.0.0.1".to_owned(),
        vec![1, 2, 3],
        vec![4, 5, 6],
    );
    let description = ClassicGroupDescription::new(
        "Stable".to_owned(),
        "consumer".to_owned(),
        "range".to_owned(),
        vec![member],
        Some(0x20),
    );

    assert_eq!(description.state(), "Stable");
    assert_eq!(description.protocol_type(), "consumer");
    assert_eq!(description.protocol_data(), "range");
    assert_eq!(description.authorized_operations(), Some(0x20));
    let member = &description.members()[0];
    assert_eq!(member.member_id(), "member-a");
    assert_eq!(member.group_instance_id(), Some("instance-a"));
    assert_eq!(member.client_id(), "client-a");
    assert_eq!(member.client_host(), "/127.0.0.1");
    assert_eq!(member.metadata(), [1, 2, 3]);
    assert_eq!(member.assignment(), [4, 5, 6]);
}
