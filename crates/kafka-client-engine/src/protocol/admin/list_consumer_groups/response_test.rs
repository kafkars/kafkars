//! Version, exact-error, scalar, and retained-capacity scenarios.

use kafka_wire::{ListGroupsResponse, list_groups_response::ListedGroup};
use kafka_wire_core::StrBytes;

use super::{ListConsumerGroupsProtocolFailure, normalize_list_consumer_groups_response};

#[test]
fn old_and_new_group_fields_cross_with_explicit_representation() {
    let mut response = ListGroupsResponse::default();
    response.groups = vec![group("alpha", "consumer", "Stable", "classic")];
    let old = normalize_list_consumer_groups_response(3, Some(3), &response, 64 * 1024)
        .unwrap_or_else(|error| panic!("old response: {error:?}"));
    let (_, old, _) = old.into_parts();
    let kafka_client_core::AdminListConsumerGroupsBrokerOutcome::Groups { groups, .. } = old else {
        panic!("groups");
    };
    assert_eq!(groups[0].group_state(), None);
    assert_eq!(groups[0].group_type(), None);

    let new = normalize_list_consumer_groups_response(3, Some(5), &response, 64 * 1024)
        .unwrap_or_else(|error| panic!("new response: {error:?}"));
    let (_, new, _) = new.into_parts();
    let kafka_client_core::AdminListConsumerGroupsBrokerOutcome::Groups { groups, .. } = new else {
        panic!("groups");
    };
    assert_eq!(groups[0].group_state(), Some("Stable"));
    assert_eq!(groups[0].group_type(), Some("classic"));
}

#[test]
fn exact_signed_broker_code_and_retained_capacity_are_strict() {
    let mut rejected = ListGroupsResponse::default();
    rejected.error_code = -17;
    let normalized = normalize_list_consumer_groups_response(9, Some(5), &rejected, 0)
        .unwrap_or_else(|error| panic!("exact rejection: {error:?}"));
    let (_, outcome, charge) = normalized.into_parts();
    assert_eq!(charge, 0);
    let kafka_client_core::AdminListConsumerGroupsBrokerOutcome::Rejected(error) = outcome else {
        panic!("rejection");
    };
    assert_eq!(error.into_parts(), (9, -17));

    let mut success = ListGroupsResponse::default();
    success.groups = vec![group("alpha", "consumer", "Stable", "classic")];
    assert_eq!(
        normalize_list_consumer_groups_response(9, Some(5), &success, 1),
        Err(ListConsumerGroupsProtocolFailure::ResponseTooLarge)
    );
}

fn group(group_id: &str, protocol_type: &str, state: &str, group_type: &str) -> ListedGroup {
    let mut group = ListedGroup::default();
    group.group_id = StrBytes::from(group_id);
    group.protocol_type = StrBytes::from(protocol_type);
    group.group_state = StrBytes::from(state);
    group.group_type = StrBytes::from(group_type);
    group
}
