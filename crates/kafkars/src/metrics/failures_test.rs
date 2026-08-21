//! Public failure-metric accessor shape.

use super::failures::FailureMetrics;

#[test]
fn classified_failure_accessors_remain_scalar_and_runtime_neutral() {
    let _: fn(FailureMetrics) -> u64 = FailureMetrics::dns;
    let _: fn(FailureMetrics) -> u64 = FailureMetrics::connect;
    let _: fn(FailureMetrics) -> u64 = FailureMetrics::transport;
    let _: fn(FailureMetrics) -> u64 = FailureMetrics::negotiation;
    let _: fn(FailureMetrics) -> u64 = FailureMetrics::authentication;
    let _: fn(FailureMetrics) -> u64 = FailureMetrics::deadline;
    let _: fn(FailureMetrics) -> u64 = FailureMetrics::local_rejection;
    let _: fn(FailureMetrics) -> u64 = FailureMetrics::response_capacity;
    let _: fn(FailureMetrics) -> u64 = FailureMetrics::route_capacity;
}
