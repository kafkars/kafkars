//! Engine error boundary around record-batch materialization failures.

use std::{error::Error, fmt};

use kafka_wire_records::RecordError;

/// Failure to turn one admitted batch into wire-owned Produce material.
#[derive(Debug)]
pub(crate) enum ProduceMaterializationError {
    /// Core supplied no records for a ready batch.
    EmptyBatch,
    /// The ordered record run cannot be represented by Kafka's signed deltas.
    RecordCountOverflow {
        /// Number of records in the engine-owned batch.
        count: usize,
    },
    /// A record timestamp cannot be expressed relative to the first timestamp.
    TimestampDeltaOverflow {
        /// First record timestamp and therefore the batch base.
        base_timestamp_ms: i64,
        /// Timestamp that could not be represented as a signed delta.
        timestamp_ms: i64,
    },
    /// An engine header-name invariant was violated before record encoding.
    InvalidHeaderName(std::str::Utf8Error),
    /// The authoritative record-layer encoder rejected the batch.
    Record(RecordError),
}

impl ProduceMaterializationError {
    pub(super) const fn empty_batch() -> Self {
        Self::EmptyBatch
    }

    pub(super) const fn record_count_overflow(count: usize) -> Self {
        Self::RecordCountOverflow { count }
    }

    pub(super) const fn timestamp_delta_overflow(
        base_timestamp_ms: i64,
        timestamp_ms: i64,
    ) -> Self {
        Self::TimestampDeltaOverflow {
            base_timestamp_ms,
            timestamp_ms,
        }
    }

    pub(super) const fn record(source: RecordError) -> Self {
        Self::Record(source)
    }

    pub(super) const fn invalid_header_name(source: std::str::Utf8Error) -> Self {
        Self::InvalidHeaderName(source)
    }

    #[cfg(test)]
    pub(super) const fn record_error(&self) -> Option<&RecordError> {
        match self {
            Self::Record(source) => Some(source),
            Self::InvalidHeaderName(_)
            | Self::EmptyBatch
            | Self::RecordCountOverflow { .. }
            | Self::TimestampDeltaOverflow { .. } => None,
        }
    }
}

impl fmt::Display for ProduceMaterializationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyBatch => formatter.write_str("cannot materialize an empty Produce batch"),
            Self::RecordCountOverflow { count } => write!(
                formatter,
                "Produce batch record count {count} exceeds Kafka's signed-delta domain"
            ),
            Self::TimestampDeltaOverflow {
                base_timestamp_ms,
                timestamp_ms,
            } => write!(
                formatter,
                "record timestamp {timestamp_ms} cannot be represented relative to batch base \
                 timestamp {base_timestamp_ms}"
            ),
            Self::Record(source) => {
                write!(
                    formatter,
                    "Kafka record-batch materialization failed: {source}"
                )
            }
            Self::InvalidHeaderName(_) => {
                formatter.write_str("producer header name lost its UTF-8 validation")
            }
        }
    }
}

impl Error for ProduceMaterializationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Record(source) => Some(source),
            Self::InvalidHeaderName(source) => Some(source),
            Self::EmptyBatch
            | Self::RecordCountOverflow { .. }
            | Self::TimestampDeltaOverflow { .. } => None,
        }
    }
}
