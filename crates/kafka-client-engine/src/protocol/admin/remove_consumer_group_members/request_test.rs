//! Generated static-member `LeaveGroup` request construction scenarios.

use kafka_client_core::{ConsumerGroupMemberRemoval, RemoveConsumerGroupMembersPlan};
use kafka_wire_core::{ApiVersion, BytesMut, DecodeLimits, Decoder, KafkaDecode, KafkaEncode};

use super::{RemoveConsumerGroupMembersRequestFailure, remove_consumer_group_members_request};

#[test]
fn request_uses_member_array_and_v5_only_when_reason_is_present() {
    let plan = plan(Some("maintenance"));
    let (request, minimum_version) = remove_consumer_group_members_request(&plan, usize::MAX)
        .unwrap_or_else(|error| panic!("valid request: {error}"));
    assert_eq!(minimum_version, 5);
    assert_eq!(request.group_id.as_str(), "payments");
    assert_eq!(request.member_id.as_str(), "");
    assert_eq!(request.members.len(), 2);
    assert_eq!(
        request.members[0]
            .group_instance_id
            .as_ref()
            .map(kafka_wire_core::StrBytes::as_str),
        Some("instance-b")
    );
    assert_eq!(
        request.members[0]
            .reason
            .as_ref()
            .map(kafka_wire_core::StrBytes::as_str),
        Some("maintenance")
    );
    let decoded = round_trip(&request, minimum_version);
    assert_eq!(decoded, request);
}

#[test]
fn reasonless_request_keeps_v3_compatibility_and_checks_budget() {
    let plan = plan(None);
    let (request, minimum_version) = remove_consumer_group_members_request(&plan, usize::MAX)
        .unwrap_or_else(|error| panic!("valid request: {error}"));
    assert_eq!(minimum_version, 3);
    assert!(request.members.iter().all(|member| member.reason.is_none()));
    assert_eq!(
        remove_consumer_group_members_request(&plan, 0).err(),
        Some(RemoveConsumerGroupMembersRequestFailure::RetainedBytes)
    );
}

fn plan(reason: Option<&str>) -> RemoveConsumerGroupMembersPlan {
    RemoveConsumerGroupMembersPlan::new(
        "payments".to_owned(),
        vec![
            ConsumerGroupMemberRemoval::new("instance-b".to_owned()),
            ConsumerGroupMemberRemoval::new("instance-a".to_owned()),
        ],
        reason.map(str::to_owned),
    )
    .unwrap_or_else(|error| panic!("valid plan: {error}"))
}

fn round_trip(
    request: &kafka_wire::LeaveGroupRequest,
    version: i16,
) -> kafka_wire::LeaveGroupRequest {
    let version = ApiVersion::new(version);
    let mut encoded = BytesMut::new();
    request
        .encode_into(&mut encoded, version)
        .unwrap_or_else(|error| panic!("request encodes: {error}"));
    let mut decoder = Decoder::new(encoded.freeze(), DecodeLimits::default())
        .unwrap_or_else(|error| panic!("request frame is bounded: {error}"));
    let decoded = kafka_wire::LeaveGroupRequest::decode(&mut decoder, version)
        .unwrap_or_else(|error| panic!("request decodes: {error}"));
    decoder
        .finish()
        .unwrap_or_else(|error| panic!("request consumes frame: {error}"));
    decoded
}
