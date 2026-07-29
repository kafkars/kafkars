//! Inert `ListTopics` builder ownership scenarios.

use std::time::Duration;

use super::ListTopicsBuilder;
use crate::{Client, DeliveryStatus, ErrorKind};

#[test]
fn list_topics_builder_is_send_before_single_submission() {
    fn assert_send<T: Send>() {}
    assert_send::<ListTopicsBuilder>();
}

#[test]
fn zero_deadline_and_internal_option_remain_inert_until_submit() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));
    let admin = client.admin();
    let builders = [
        admin
            .list_topics()
            .include_authorized_operations(true)
            .include_internal(true),
        admin
            .list_topics()
            .include_internal(true)
            .include_authorized_operations(true),
    ];

    for builder in builders {
        let error = builder
            .deadline_after(Duration::ZERO)
            .submit()
            .wait()
            .err()
            .unwrap_or_else(|| panic!("zero deadline must reject at submit"));
        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
    }
}
