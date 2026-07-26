//! Exact Fetch isolation wire-value scenarios.

use super::FetchIsolation;

#[test]
fn closed_isolation_values_match_kafka() {
    assert_eq!(FetchIsolation::ReadUncommitted.wire_value(), 0);
    assert_eq!(FetchIsolation::ReadCommitted.wire_value(), 1);
}
