//! Public share builder policy and exact rejection ownership.

use std::time::Duration;

use super::{ShareConsumerBuilder, ShareConsumerFetchConfig};
use crate::{Client, ErrorKind};

fn client() -> Client {
    Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("lazy client start: {error}"))
}

#[test]
fn builder_retains_group_rack_subscription_and_bounded_deadlines() {
    let builder = client()
        .share_consumer("workers")
        .rack("rack-a")
        .subscribe(["orders", "payments"])
        .fetch_config(
            ShareConsumerFetchConfig::default()
                .with_max_records(32)
                .with_batch_size(8),
        )
        .membership_start_timeout(Duration::from_secs(11))
        .close_timeout(Duration::from_secs(13));

    assert_eq!(builder.group_id(), "workers");
    assert_eq!(builder.selected_rack(), Some("rack-a"));
    assert_eq!(builder.subscription(), ["orders", "payments"]);
    assert_eq!(builder.selected_fetch_config().max_records(), 32);
    assert_eq!(builder.selected_fetch_config().batch_size(), 8);
    assert_eq!(
        builder.selected_membership_start_timeout(),
        Duration::from_secs(11)
    );
    assert_eq!(builder.selected_close_timeout(), Duration::from_secs(13));
}

#[test]
fn invalid_fetch_policy_returns_the_exact_consumed_builder() {
    let fetch = ShareConsumerFetchConfig::default().with_max_records(0);
    let rejected = client()
        .share_consumer("workers")
        .subscribe(["orders"])
        .fetch_config(fetch)
        .build()
        .err()
        .unwrap_or_else(|| panic!("zero records must reject"));

    assert_eq!(rejected.error().kind(), ErrorKind::Configuration);
    let (builder, error) = rejected.into_parts();
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(builder.selected_fetch_config(), fetch);
}

#[test]
fn invalid_deadline_returns_the_exact_consumed_builder() {
    let rejected = client()
        .share_consumer("workers")
        .rack("rack-a")
        .subscribe(["orders"])
        .membership_start_timeout(Duration::ZERO)
        .build()
        .err()
        .unwrap_or_else(|| panic!("zero membership timeout must reject"));

    assert_eq!(rejected.error().kind(), ErrorKind::Configuration);
    let (builder, error) = rejected.into_parts();
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert_eq!(builder.group_id(), "workers");
    assert_eq!(builder.selected_rack(), Some("rack-a"));
    assert_eq!(builder.subscription(), ["orders"]);
}

const _: Option<ShareConsumerBuilder> = None;
