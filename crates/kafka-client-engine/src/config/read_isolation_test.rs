//! Exhaustive engine-to-core read-isolation translation scenarios.

use super::read_isolation::ConsumerReadIsolation;

#[test]
fn default_and_each_closed_value_map_to_core_policy() {
    assert_eq!(
        ConsumerReadIsolation::default(),
        ConsumerReadIsolation::ReadUncommitted
    );
    for (configured, expected) in [
        (
            ConsumerReadIsolation::ReadUncommitted,
            kafka_client_core::ReadIsolation::ReadUncommitted,
        ),
        (
            ConsumerReadIsolation::ReadCommitted,
            kafka_client_core::ReadIsolation::ReadCommitted,
        ),
    ] {
        assert_eq!(configured.core(), expected);
    }
}
