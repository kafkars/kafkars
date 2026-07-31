//! Public latency-summary accessor shape.

use std::time::Duration;

use super::latency::{LatencyMetric, LatencyMetrics};

#[test]
fn latency_accessors_preserve_stage_and_summary_shapes() {
    let _: fn(LatencyMetrics) -> LatencyMetric = LatencyMetrics::mailbox;
    let _: fn(LatencyMetrics) -> LatencyMetric = LatencyMetrics::routing;
    let _: fn(LatencyMetrics) -> LatencyMetric = LatencyMetrics::preparation;
    let _: fn(LatencyMetrics) -> LatencyMetric = LatencyMetrics::writer_admission;
    let _: fn(LatencyMetrics) -> LatencyMetric = LatencyMetrics::in_flight;
    let _: fn(LatencyMetrics) -> LatencyMetric = LatencyMetrics::end_to_end;
    let _: fn(LatencyMetrics) -> LatencyMetric = LatencyMetrics::deadline_lateness;
    let _: fn(LatencyMetric) -> u64 = LatencyMetric::samples;
    let _: fn(LatencyMetric) -> Duration = LatencyMetric::total;
    let _: fn(LatencyMetric) -> Duration = LatencyMetric::max;
}
