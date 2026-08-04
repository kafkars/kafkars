//! Classic-group timing compilation and rejection evidence.

use std::time::Duration;

use super::classic_group_config::{ClassicGroupConfigError, EngineClassicGroupConfig};

#[test]
fn default_and_distinct_values_compile_exactly() {
    let default = EngineClassicGroupConfig::default()
        .validate()
        .unwrap_or_else(|error| panic!("default classic timing: {error:?}"));
    assert_eq!(default.timing().session_timeout_ms(), 10_000);
    assert_eq!(default.timing().rebalance_timeout_ms(), 30_000);
    assert_eq!(default.heartbeat().interval_ticks(), 3_000_000_000);
    assert_eq!(default.heartbeat().attempt_timeout_ticks(), 10_000_000_000);
    assert_eq!(default.rejoin().backoff_ticks(), 1_000_000_000);
    assert_eq!(default.rejoin().attempt_timeout_ticks(), 30_000_000_000);

    let distinct = EngineClassicGroupConfig::new(
        Duration::from_millis(11_000),
        Duration::from_millis(31_000),
        Duration::from_nanos(4),
        Duration::from_nanos(12),
        Duration::from_nanos(2),
        Duration::from_nanos(32),
    )
    .validate()
    .unwrap_or_else(|error| panic!("distinct classic timing: {error:?}"));
    assert_eq!(distinct.timing().session_timeout_ms(), 11_000);
    assert_eq!(distinct.timing().rebalance_timeout_ms(), 31_000);
    assert_eq!(distinct.heartbeat().interval_ticks(), 4);
    assert_eq!(distinct.heartbeat().attempt_timeout_ticks(), 12);
    assert_eq!(distinct.rejoin().backoff_ticks(), 2);
    assert_eq!(distinct.rejoin().attempt_timeout_ticks(), 32);
}

#[test]
fn zero_fractional_wire_and_unrepresentable_tick_values_reject() {
    let default = EngineClassicGroupConfig::default();
    let cases = [
        (
            EngineClassicGroupConfig::new(
                Duration::ZERO,
                default.rebalance_timeout(),
                default.heartbeat_interval(),
                default.heartbeat_attempt_timeout(),
                default.rejoin_backoff(),
                default.rejoin_attempt_timeout(),
            ),
            ClassicGroupConfigError::SessionTimeout,
        ),
        (
            EngineClassicGroupConfig::new(
                default.session_timeout(),
                Duration::from_micros(1),
                default.heartbeat_interval(),
                default.heartbeat_attempt_timeout(),
                default.rejoin_backoff(),
                default.rejoin_attempt_timeout(),
            ),
            ClassicGroupConfigError::RebalanceTimeout,
        ),
        (
            EngineClassicGroupConfig::new(
                default.session_timeout(),
                default.rebalance_timeout(),
                Duration::MAX,
                default.heartbeat_attempt_timeout(),
                default.rejoin_backoff(),
                default.rejoin_attempt_timeout(),
            ),
            ClassicGroupConfigError::HeartbeatInterval,
        ),
    ];

    for (config, expected) in cases {
        assert_eq!(config.validate(), Err(expected));
    }
}
