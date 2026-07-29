//! Inert legacy replacement builder ownership and submission scenarios.

use std::time::Duration;

use super::{
    LegacyReplaceTopicConfigsBuilder, LegacyTopicConfigEntry, LegacyTopicConfigReplacement,
};
use crate::{Client, DeliveryStatus, ErrorKind};

#[test]
fn legacy_replacement_builder_is_send_before_single_submission() {
    fn assert_send<T: Send>() {}
    assert_send::<LegacyReplaceTopicConfigsBuilder>();
}

#[test]
fn duplicate_topics_and_keys_are_rejected_only_at_submit() {
    let builder = client()
        .admin()
        .legacy_replace_topic_configs([
            LegacyTopicConfigReplacement::new(
                "orders",
                [
                    LegacyTopicConfigEntry::set("cleanup.policy", "compact"),
                    LegacyTopicConfigEntry::restore_default("cleanup.policy"),
                ],
            ),
            LegacyTopicConfigReplacement::new("orders", []),
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
        .legacy_replace_topic_configs([LegacyTopicConfigReplacement::new("orders", [])])
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
