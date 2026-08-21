//! Inert `DescribeConfigs` builder ownership scenarios.

use std::time::Duration;

use super::DescribeConfigsBuilder;
use crate::{Client, DeliveryStatus, ErrorKind, TopicConfigQuery};

#[test]
fn describe_configs_builder_is_send_before_single_submission() {
    fn assert_send<T: Send>() {}
    assert_send::<DescribeConfigsBuilder>();
}

#[test]
fn duplicate_topic_queries_are_rejected_only_at_submit() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));
    let builder = client
        .admin()
        .describe_configs([
            TopicConfigQuery::new("orders").configuration_keys(["cleanup.policy"]),
            TopicConfigQuery::new("orders").configuration_keys(["retention.ms"]),
        ])
        .include_synonyms(true)
        .include_documentation(true);
    let error = builder
        .submit()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("duplicate topics must reject at submit"));
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}

#[test]
fn zero_deadline_is_rejected_at_the_public_submission_boundary() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"));
    let error = client
        .admin()
        .describe_configs(["orders"])
        .deadline_after(Duration::ZERO)
        .submit()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("zero deadline must reject at submit"));
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}
