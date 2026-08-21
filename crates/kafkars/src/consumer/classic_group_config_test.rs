//! Public classic-group timing defaults and replacement evidence.

use std::time::Duration;

use super::ClassicGroupConfig;

#[test]
fn defaults_match_the_existing_classic_membership_policy() {
    let config = ClassicGroupConfig::default();

    assert_eq!(config.session_timeout(), Duration::from_secs(10));
    assert_eq!(config.rebalance_timeout(), Duration::from_secs(30));
    assert_eq!(config.heartbeat_interval(), Duration::from_secs(3));
    assert_eq!(config.heartbeat_attempt_timeout(), Duration::from_secs(10));
    assert_eq!(config.rejoin_backoff(), Duration::from_secs(1));
    assert_eq!(config.rejoin_attempt_timeout(), Duration::from_secs(30));
}

#[test]
fn every_timing_value_is_replaced_independently() {
    let config = ClassicGroupConfig::default()
        .with_session_timeout(Duration::from_secs(11))
        .with_rebalance_timeout(Duration::from_secs(31))
        .with_heartbeat_interval(Duration::from_secs(4))
        .with_heartbeat_attempt_timeout(Duration::from_secs(12))
        .with_rejoin_backoff(Duration::from_secs(2))
        .with_rejoin_attempt_timeout(Duration::from_secs(32));

    assert_eq!(config.session_timeout(), Duration::from_secs(11));
    assert_eq!(config.rebalance_timeout(), Duration::from_secs(31));
    assert_eq!(config.heartbeat_interval(), Duration::from_secs(4));
    assert_eq!(config.heartbeat_attempt_timeout(), Duration::from_secs(12));
    assert_eq!(config.rejoin_backoff(), Duration::from_secs(2));
    assert_eq!(config.rejoin_attempt_timeout(), Duration::from_secs(32));
}
