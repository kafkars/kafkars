//! Transaction initialization default scenarios.

use std::time::Duration;

use super::EngineConfig;

#[test]
fn transaction_defaults_keep_operation_and_broker_time_distinct() {
    let config = EngineConfig::new(vec!["broker.test:9092".to_owned()]);

    assert_eq!(
        config.transaction_initialization_timeout(),
        Duration::from_secs(30)
    );
    assert_eq!(config.transaction_timeout(), Duration::from_secs(60));
}
