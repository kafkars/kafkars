//! Stable engine-side producer compression intent and core translation.

/// Closed producer compression choices supported by `kafka-wire-records`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ProducerCompression {
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

impl ProducerCompression {
    pub(crate) const fn core(self) -> kafka_client_core::CompressionPolicy {
        match self {
            Self::None => kafka_client_core::CompressionPolicy::None,
            Self::Gzip => kafka_client_core::CompressionPolicy::Gzip,
            Self::Snappy => kafka_client_core::CompressionPolicy::Snappy,
            Self::Lz4 => kafka_client_core::CompressionPolicy::Lz4,
            Self::Zstd => kafka_client_core::CompressionPolicy::Zstd,
        }
    }
}
