//! Public group-consumer configuration validation and exact recovery scenarios.

use std::time::Duration;

use super::{ClassicGroupConfig, ConsumerGroupProtocol, GroupConsumerOperationConfig};
use crate::{Client, ErrorKind};

#[test]
fn classic_timing_is_group_scoped_and_invalid_policy_returns_the_exact_builder() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("lazy client start: {error}"));
    let config = ClassicGroupConfig::default()
        .with_session_timeout(Duration::from_secs(11))
        .with_rebalance_timeout(Duration::from_secs(31))
        .with_heartbeat_interval(Duration::from_secs(4))
        .with_heartbeat_attempt_timeout(Duration::from_secs(12))
        .with_rejoin_backoff(Duration::from_secs(2))
        .with_rejoin_attempt_timeout(Duration::from_secs(32));
    assert_eq!(
        client
            .consumer("configured-workers")
            .classic_group_config(config)
            .selected_classic_group_config(),
        config
    );

    let invalid = config.with_session_timeout(Duration::ZERO);
    let rejected = client
        .consumer("configured-workers")
        .subscribe(["orders"])
        .classic_group_config(invalid)
        .build()
        .err()
        .unwrap_or_else(|| panic!("zero classic session timeout must reject"));
    assert_eq!(rejected.error().kind(), ErrorKind::Configuration);
    assert_eq!(rejected.builder().selected_classic_group_config(), invalid);

    let rejected = client
        .consumer("modern-workers")
        .subscribe(["orders"])
        .group_protocol(ConsumerGroupProtocol::Consumer)
        .classic_group_config(config)
        .build()
        .err()
        .unwrap_or_else(|| panic!("modern membership cannot consume classic timing"));
    assert_eq!(rejected.error().kind(), ErrorKind::Configuration);
    assert_eq!(rejected.builder().selected_classic_group_config(), config);
}

#[test]
fn membership_start_timeout_is_captured_at_build_and_recovered_on_rejection() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("lazy client start: {error}"));
    let rejected = client
        .consumer("workers")
        .subscribe(["orders"])
        .membership_start_timeout(Duration::ZERO)
        .build()
        .err()
        .unwrap_or_else(|| panic!("zero membership-start timeout must reject"));

    assert_eq!(rejected.error().kind(), ErrorKind::Configuration);
    assert_eq!(
        rejected.builder().selected_membership_start_timeout(),
        Duration::ZERO
    );
}

#[test]
fn hosted_group_operation_durations_are_group_scoped_and_recovered_exactly() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("lazy client start: {error}"));
    let config =
        GroupConsumerOperationConfig::new(Duration::from_secs(11), Duration::from_secs(17));
    let selected = client
        .consumer("configured-workers")
        .operation_config(config);
    assert_eq!(selected.selected_operation_config(), config);
    assert_eq!(selected.selected_seek_timeout(), Duration::from_secs(11));
    assert_eq!(selected.selected_close_timeout(), Duration::from_secs(17));

    let selected = client
        .consumer("configured-workers")
        .seek_timeout(Duration::from_secs(13))
        .close_timeout(Duration::from_secs(19));
    assert_eq!(
        selected.selected_operation_config(),
        GroupConsumerOperationConfig::new(Duration::from_secs(13), Duration::from_secs(19))
    );

    for invalid in [
        config.with_seek_timeout(Duration::ZERO),
        config.with_close_timeout(Duration::MAX),
    ] {
        let rejected = client
            .consumer("configured-workers")
            .subscribe(["orders"])
            .operation_config(invalid)
            .build()
            .err()
            .unwrap_or_else(|| panic!("invalid group operation policy must reject"));
        assert_eq!(rejected.error().kind(), ErrorKind::Configuration);
        assert_eq!(rejected.builder().selected_operation_config(), invalid);
    }
}
