//! Engine producer-metric accessor shape.

use super::EngineProducerMetrics;

#[test]
fn producer_pressure_accessors_remain_fixed_size_scalars() {
    let _: fn(EngineProducerMetrics) -> usize = EngineProducerMetrics::active_records;
    let _: fn(EngineProducerMetrics) -> usize = EngineProducerMetrics::active_bytes;
    let _: fn(EngineProducerMetrics) -> usize = EngineProducerMetrics::waiting_records;
    let _: fn(EngineProducerMetrics) -> usize = EngineProducerMetrics::waiting_bytes;
    let _: fn(EngineProducerMetrics) -> usize = EngineProducerMetrics::prepared_batches;
    let _: fn(EngineProducerMetrics) -> usize = EngineProducerMetrics::prepared_batch_bytes;
    let _: fn(EngineProducerMetrics) -> usize = EngineProducerMetrics::terminal_backlog;
    let _: fn(EngineProducerMetrics) -> u64 = EngineProducerMetrics::produce_requests;
    let _: fn(EngineProducerMetrics) -> u64 = EngineProducerMetrics::produce_batches;
    let _: fn(EngineProducerMetrics) -> u64 = EngineProducerMetrics::produce_records;
    let _: fn(EngineProducerMetrics) -> u64 = EngineProducerMetrics::produce_encoded_bytes;
    let _: fn(EngineProducerMetrics) -> bool = EngineProducerMetrics::accepting;
    let _: fn(EngineProducerMetrics) -> bool = EngineProducerMetrics::healthy;
}
