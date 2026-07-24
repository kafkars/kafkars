//! Public producer compression vocabulary without protocol implementation types.

/// `RecordBatch` compression selected for every producer owned by one client.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Compression {
    /// Keep the `RecordBatch` records payload uncompressed.
    #[default]
    None,
    /// Use Kafka-compatible gzip framing.
    Gzip,
    /// Use Kafka's xerial snappy framing.
    Snappy,
    /// Use Kafka-compatible LZ4 framing.
    Lz4,
    /// Use Kafka-compatible zstd framing.
    Zstd,
}
