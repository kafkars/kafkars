//! Request materialization stays fenced by exact prepared membership state.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{GroupId, MemberId, ShareGroupHeartbeatPolicy, TopicId};

use super::{ShareMembershipCatalog, ShareMembershipInterpreter, ShareTopicIdentity};

#[test]
fn join_materialization_retains_stable_member_rack_and_subscription() {
    let clock = crate::clock::MonotonicClock::new();
    let mut owner = owner();
    let capture = clock
        .capture_deadline_after(Duration::from_secs(30))
        .unwrap_or_else(|error| panic!("capture: {error:?}"));
    owner
        .begin(capture)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    let request = owner
        .prepare_request()
        .unwrap_or_else(|error| panic!("request: {error:?}"))
        .into_generated_request();
    assert_eq!(request.group_id.as_str(), "share-group");
    assert_eq!(request.member_id.as_str(), "stable-member");
    assert_eq!(
        request
            .rack_id
            .as_ref()
            .map(kafka_wire_core::StrBytes::as_str),
        Some("r1")
    );
    assert_eq!(request.member_epoch, 0);
    assert_eq!(
        request.subscribed_topic_names.as_ref().map(Vec::len),
        Some(1)
    );
    assert_eq!(
        request
            .subscribed_topic_names
            .as_ref()
            .map(|topics| topics[0].as_str()),
        Some("orders")
    );
}

pub(super) fn owner() -> ShareMembershipInterpreter {
    let catalog = ShareMembershipCatalog::try_new(
        Arc::from("share-group"),
        Arc::from("stable-member"),
        Some(Arc::from("r1")),
        vec![ShareTopicIdentity::new(
            TopicId::from_raw(1),
            Arc::from("orders"),
            [7; 16],
            2,
        )],
    )
    .unwrap_or_else(|error| panic!("catalog: {error:?}"));
    ShareMembershipInterpreter::new(
        GroupId::try_from_raw(1).unwrap_or_else(|| panic!("group id")),
        MemberId::try_from_raw(1).unwrap_or_else(|| panic!("member id")),
        ShareGroupHeartbeatPolicy::try_new(10_000_000_000)
            .unwrap_or_else(|error| panic!("policy: {error:?}")),
        catalog,
    )
}
