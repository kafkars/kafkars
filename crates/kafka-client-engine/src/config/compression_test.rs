//! Engine compression default and exhaustive core translation scenarios.

use super::compression::ProducerCompression;

#[test]
fn every_engine_choice_maps_to_one_closed_core_policy() {
    let pairs = [
        (
            ProducerCompression::None,
            kafka_client_core::CompressionPolicy::None,
        ),
        (
            ProducerCompression::Gzip,
            kafka_client_core::CompressionPolicy::Gzip,
        ),
        (
            ProducerCompression::Snappy,
            kafka_client_core::CompressionPolicy::Snappy,
        ),
        (
            ProducerCompression::Lz4,
            kafka_client_core::CompressionPolicy::Lz4,
        ),
        (
            ProducerCompression::Zstd,
            kafka_client_core::CompressionPolicy::Zstd,
        ),
    ];
    for (engine, core) in pairs {
        assert_eq!(engine.core(), core);
    }
}

#[test]
fn engine_compression_defaults_to_none() {
    assert_eq!(ProducerCompression::default(), ProducerCompression::None);
}
