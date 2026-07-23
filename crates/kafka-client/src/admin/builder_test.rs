//! Inert builder ownership scenarios.

use std::time::Duration;

use super::{CreateTopicsBuilder, NewTopic};
use crate::{Client, DeliveryStatus, ErrorKind};

#[test]
fn create_topics_builder_is_send_before_single_submission() {
    fn assert_send<T: Send>() {}
    assert_send::<CreateTopicsBuilder>();
}

#[test]
fn empty_zero_deadline_builder_is_inert_until_submit() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));
    let builder = client
        .admin()
        .create_topics(std::iter::empty::<NewTopic>())
        .deadline_after(Duration::ZERO);

    let error = builder
        .submit()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("zero deadline must reject at submit"));
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}
