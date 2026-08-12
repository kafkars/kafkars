//! Public producer-metric accessor shape.

use super::producer::ProducerMetrics;

#[test]
fn producer_pressure_accessors_remain_scalar_and_runtime_neutral() {
    let _: fn(ProducerMetrics) -> usize = ProducerMetrics::active_records;
    let _: fn(ProducerMetrics) -> usize = ProducerMetrics::active_bytes;
    let _: fn(ProducerMetrics) -> usize = ProducerMetrics::waiting_records;
    let _: fn(ProducerMetrics) -> usize = ProducerMetrics::waiting_bytes;
    let _: fn(ProducerMetrics) -> usize = ProducerMetrics::prepared_batches;
    let _: fn(ProducerMetrics) -> usize = ProducerMetrics::prepared_batch_bytes;
    let _: fn(ProducerMetrics) -> usize = ProducerMetrics::terminal_backlog;
    let _: fn(ProducerMetrics) -> u64 = ProducerMetrics::produce_requests;
    let _: fn(ProducerMetrics) -> u64 = ProducerMetrics::produce_batches;
    let _: fn(ProducerMetrics) -> u64 = ProducerMetrics::produce_records;
    let _: fn(ProducerMetrics) -> u64 = ProducerMetrics::produce_encoded_bytes;
    let _: fn(ProducerMetrics) -> bool = ProducerMetrics::accepting;
    let _: fn(ProducerMetrics) -> bool = ProducerMetrics::healthy;
}
