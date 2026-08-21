//! Generic destructive replacement builder ownership and validation scenarios.
#![expect(
    clippy::expect_used,
    reason = "the test asserts a specific validation failure"
)]

use std::time::Duration;

use crate::{
    Client, ConfigResourceType, DeliveryStatus, ErrorKind, LegacyConfigResourceReplacement,
};

use super::LegacyReplaceConfigResourcesBuilder;

#[test]
fn builder_is_send_and_nonpositive_types_reject_definitely_unsent() {
    fn assert_send<T: Send>() {}
    assert_send::<LegacyReplaceConfigResourcesBuilder>();

    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:9092"])
        .build()
        .unwrap_or_else(|error| panic!("build local client: {error}"));
    let error = client
        .admin()
        .legacy_replace_config_resources([LegacyConfigResourceReplacement::new(
            ConfigResourceType::from_raw(0),
            "invalid",
            [],
        )])
        .validate_only(true)
        .deadline_after(Duration::from_secs(1))
        .submit()
        .wait()
        .expect_err("nonpositive resource type must reject before API 33 transport");
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}
