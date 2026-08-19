//! Generic `DescribeConfigs` builder ownership and deadline scenarios.
#![expect(
    clippy::expect_used,
    reason = "the test asserts a specific validation failure"
)]

use std::time::Duration;

use crate::{Client, ConfigResourceQuery, ConfigResourceType, DeliveryStatus, ErrorKind};

use super::DescribeConfigResourcesBuilder;

#[test]
fn generic_builder_is_send_and_rejects_nonpositive_types_at_submit() {
    fn assert_send<T: Send>() {}
    assert_send::<DescribeConfigResourcesBuilder>();

    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:9092"])
        .build()
        .unwrap_or_else(|error| panic!("build local client: {error}"));
    let error = client
        .admin()
        .describe_config_resources([ConfigResourceQuery::new(
            ConfigResourceType::from_raw(0),
            "invalid",
        )])
        .deadline_after(Duration::from_secs(1))
        .submit()
        .wait()
        .expect_err("nonpositive resource type must fail before transport");
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}
