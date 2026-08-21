//! General group listing builder ownership evidence.

use std::time::Duration;

use super::ListGroupsBuilder;
use crate::{Client, DeliveryStatus, ErrorKind};

#[test]
fn builder_is_send_before_single_submission() {
    fn assert_send<T: Send>() {}
    assert_send::<ListGroupsBuilder>();
}

#[test]
fn public_handle_keeps_zero_deadline_inert_until_submit() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));
    let error = client
        .admin()
        .list_groups()
        .deadline_after(Duration::ZERO)
        .submit()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("zero deadline must reject at submit"));

    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}

#[test]
fn invalid_filters_reject_at_submit_before_delivery() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));
    let invalid = [
        client.admin().list_groups().state_filters([""]).submit(),
        client
            .admin()
            .list_groups()
            .group_type_filters(["consumer", "consumer"])
            .submit(),
        client
            .admin()
            .list_groups()
            .protocol_type_filters(["x".repeat(i16::MAX as usize + 1)])
            .submit(),
    ];

    for operation in invalid {
        let error = operation
            .wait()
            .err()
            .unwrap_or_else(|| panic!("invalid filters must reject at submit"));
        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
    }
}
