//! Transaction bridge duration representation scenarios.

use std::time::Duration;

use super::handle::transaction_timeout_ms;

#[test]
fn broker_timeout_conversion_defers_policy_to_engine_and_core() {
    assert_eq!(transaction_timeout_ms(Duration::ZERO), 0);
    assert_eq!(
        transaction_timeout_ms(Duration::from_millis(45_000)),
        45_000
    );
    assert_eq!(transaction_timeout_ms(Duration::MAX), u32::MAX);
}
