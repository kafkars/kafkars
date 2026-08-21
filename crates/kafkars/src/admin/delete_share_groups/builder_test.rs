//! Public shape checks for inert share-group deletion options.

use std::time::Duration;

use super::DeleteShareGroupsBuilder;
use crate::{Client, DeliveryStatus, ErrorKind};

#[test]
fn builder_is_thread_transferable() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<DeleteShareGroupsBuilder>();
}

#[test]
fn public_handle_keeps_zero_deadline_inert_until_submit() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));
    let error = client
        .admin()
        .delete_share_groups(["share-workers"])
        .deadline_after(Duration::ZERO)
        .submit()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("zero deadline must reject at submit"));

    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}
