//! Public call-metric accessor shape.

use super::calls::CallMetrics;

#[test]
fn cumulative_call_accessors_remain_scalar_and_runtime_neutral() {
    let _: fn(CallMetrics) -> u64 = CallMetrics::admitted;
    let _: fn(CallMetrics) -> u64 = CallMetrics::succeeded;
    let _: fn(CallMetrics) -> u64 = CallMetrics::failed;
    let _: fn(CallMetrics) -> u64 = CallMetrics::observer_abandoned;
    let _: fn(CallMetrics) -> u64 = CallMetrics::not_sent;
    let _: fn(CallMetrics) -> u64 = CallMetrics::possibly_sent;
}
