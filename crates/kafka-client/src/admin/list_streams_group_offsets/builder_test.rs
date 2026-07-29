//! Streams-group builder delegation and submission-boundary evidence.

use std::time::Duration;

use crate::{Client, DeliveryStatus, ErrorKind};

use super::ListStreamsGroupOffsetsBuilder;

#[test]
fn builder_is_send_before_single_submission() {
    fn assert_send<T: Send>() {}
    assert_send::<ListStreamsGroupOffsetsBuilder>();
}

#[test]
fn zero_deadline_and_stability_remain_inert_until_delegated_submit() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));
    let error = client
        .admin()
        .list_streams_group_offsets("streams-payments")
        .require_stable(true)
        .deadline_after(Duration::ZERO)
        .submit()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("zero deadline must reject at delegated submit"));
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}
