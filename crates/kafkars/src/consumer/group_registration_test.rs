//! Lossless group-builder rejection under transient pre-admission contention.

use std::time::{Duration, Instant};

use super::{
    ClassicGroupAssignor, ConsumerBuildError, ConsumerBuilder, ConsumerFetchConfig,
    ConsumerGroupProtocol, ConsumerLimits, OffsetReset, ReadIsolation,
};
use crate::{Client, ErrorKind, RetryAdvice};

const REJECTION_TIMEOUT: Duration = Duration::from_secs(10);

fn build_until_non_retryable(mut builder: ConsumerBuilder) -> ConsumerBuildError {
    let deadline = Instant::now() + REJECTION_TIMEOUT;
    loop {
        match builder.build() {
            Ok(_consumer) => panic!("invalid group-consumer registration was accepted"),
            Err(rejected) if rejected.error().retry_advice() == RetryAdvice::RetrySafe => {
                let (returned, transient) = rejected.into_parts();
                assert_eq!(transient.kind(), ErrorKind::Backpressure);
                assert!(
                    Instant::now() < deadline,
                    "transient group registration did not settle: {transient}"
                );
                builder = returned;
                std::hint::spin_loop();
            }
            Err(rejected) => return rejected,
        }
    }
}

#[test]
fn limits_are_group_scoped_and_invalid_limits_return_the_exact_builder() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("lazy client start: {error}"));
    let limits = ConsumerLimits::new(3, 5, 4 * 1024 * 1024, 1024 * 1024);
    assert_eq!(
        client
            .consumer("configured-workers")
            .limits(limits)
            .selected_limits(),
        limits
    );

    let invalid = limits.with_in_flight_fetches(0);
    let rejected = build_until_non_retryable(
        client
            .consumer("configured-workers")
            .subscribe(["orders"])
            .limits(invalid),
    );
    assert_eq!(rejected.error().kind(), ErrorKind::Configuration);
    assert_eq!(rejected.builder().selected_limits(), invalid);
}

#[test]
fn fetch_policy_is_group_scoped_and_invalid_policy_returns_the_exact_builder() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("lazy client start: {error}"));
    let fetch = ConsumerFetchConfig::default()
        .with_max_wait(Duration::from_millis(125))
        .with_min_bytes(4_096)
        .with_max_bytes(2 * 1024 * 1024)
        .with_partition_max_bytes(512 * 1024)
        .with_attempt_timeout(Duration::from_secs(9));
    assert_eq!(
        client
            .consumer("configured-workers")
            .fetch_config(fetch)
            .selected_fetch_config(),
        fetch
    );

    let invalid = fetch.with_max_wait(Duration::ZERO);
    let rejected = build_until_non_retryable(
        client
            .consumer("configured-workers")
            .subscribe(["orders"])
            .fetch_config(invalid),
    );
    assert_eq!(rejected.error().kind(), ErrorKind::Configuration);
    assert_eq!(rejected.builder().selected_fetch_config(), invalid);
}

#[test]
fn consumer_protocol_rejects_an_explicit_classic_assignor_in_both_orders() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("lazy client start: {error}"));
    let builders = [
        client
            .consumer("modern-workers")
            .group_protocol(ConsumerGroupProtocol::Consumer)
            .classic_group_assignor(ClassicGroupAssignor::CooperativeSticky),
        client
            .consumer("modern-workers")
            .classic_group_assignor(ClassicGroupAssignor::CooperativeSticky)
            .group_protocol(ConsumerGroupProtocol::Consumer),
    ];

    for builder in builders {
        let rejected = build_until_non_retryable(builder.subscribe(["orders"]));
        assert_eq!(rejected.error().kind(), ErrorKind::Configuration);
        let (builder, error) = rejected.into_parts();
        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert_eq!(builder.group_id(), "modern-workers");
        assert_eq!(builder.subscription(), ["orders"]);
        assert_eq!(
            builder.selected_group_protocol(),
            ConsumerGroupProtocol::Consumer
        );
        assert_eq!(builder.selected_classic_group_assignor(), None);
        assert_eq!(
            builder
                .group_protocol(ConsumerGroupProtocol::Classic)
                .selected_classic_group_assignor(),
            Some(ClassicGroupAssignor::CooperativeSticky)
        );
    }
}

#[test]
fn static_identity_is_opt_in_and_exactly_recovered_on_invalid_registration() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("lazy client start: {error}"));
    assert_eq!(
        client
            .consumer("dynamic-workers")
            .selected_group_instance_id(),
        None
    );
    let builder = client
        .consumer("static-workers")
        .group_instance_id("instance-a");
    assert_eq!(builder.selected_group_instance_id(), Some("instance-a"));

    let rejected = build_until_non_retryable(
        client
            .consumer("static-workers")
            .group_instance_id("")
            .subscribe(["orders"])
            .classic_group_assignor(ClassicGroupAssignor::CooperativeSticky)
            .on_missing_offset(OffsetReset::Latest)
            .read_isolation(ReadIsolation::ReadCommitted)
            .processing_timeout(Duration::from_secs(41)),
    );
    assert_eq!(rejected.error().kind(), ErrorKind::Configuration);
    assert_eq!(rejected.builder().group_id(), "static-workers");
    assert_eq!(rejected.builder().selected_group_instance_id(), Some(""));
    assert_eq!(rejected.builder().subscription(), ["orders"]);
    assert_eq!(
        rejected.builder().selected_classic_group_assignor(),
        Some(ClassicGroupAssignor::CooperativeSticky)
    );
    assert_eq!(rejected.builder().offset_reset(), OffsetReset::Latest);
    assert_eq!(
        rejected.builder().selected_read_isolation(),
        ReadIsolation::ReadCommitted
    );
    assert_eq!(
        rejected.builder().selected_processing_timeout(),
        Duration::from_secs(41)
    );
}
