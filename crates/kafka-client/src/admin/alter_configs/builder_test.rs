//! Inert incremental configuration builder ownership scenarios.

use std::time::Duration;

use super::{ConfigAlteration, IncrementalAlterConfigsBuilder, TopicConfigAlterations};
use crate::{Client, DeliveryStatus, ErrorKind};

#[test]
fn builder_is_send_before_single_submission() {
    fn assert_send<T: Send>() {}
    assert_send::<IncrementalAlterConfigsBuilder>();
}

#[test]
fn duplicate_topics_and_keys_are_rejected_only_at_submit() {
    let client = client();
    let builder = client
        .admin()
        .incremental_alter_configs([
            TopicConfigAlterations::new(
                "orders",
                [
                    ConfigAlteration::set("cleanup.policy", ""),
                    ConfigAlteration::delete("cleanup.policy"),
                ],
            ),
            TopicConfigAlterations::new(
                "orders",
                [ConfigAlteration::append("cleanup.policy", "compact")],
            ),
        ])
        .validate_only(true);
    let error = builder
        .submit()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("duplicate identities must reject at submit"));
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}

#[test]
fn zero_deadline_is_rejected_at_the_public_submission_boundary() {
    let error = client()
        .admin()
        .incremental_alter_configs([TopicConfigAlterations::new(
            "orders",
            [ConfigAlteration::delete("retention.ms")],
        )])
        .deadline_after(Duration::ZERO)
        .submit()
        .wait()
        .err()
        .unwrap_or_else(|| panic!("zero deadline must reject at submit"));
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}

fn client() -> Client {
    Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start facade client: {error}"))
}
