//! Request construction scenarios for Admin `DeleteConsumerGroups`.

use kafka_client_core::DeleteConsumerGroupsTarget;

use super::delete_consumer_groups_request;

#[test]
fn request_contains_exactly_one_requested_group() {
    let target = DeleteConsumerGroupsTarget::new("orders-workers".to_owned());
    let request = delete_consumer_groups_request(&target, usize::MAX)
        .unwrap_or_else(|_| panic!("request must fit"));

    assert_eq!(request.groups_names.len(), 1);
    assert_eq!(request.groups_names[0].as_str(), "orders-workers");
}
