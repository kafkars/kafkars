//! Engine error boundary around record-batch materialization failures.

use std::{error::Error, fmt};

use kafka_wire_records::RecordError;

/// Failure to turn one admitted record into wire-owned Produce material.
#[derive(Debug)]
pub(crate) struct ProduceMaterializationError {
    source: RecordError,
}

impl ProduceMaterializationError {
    pub(super) const fn new(source: RecordError) -> Self {
        Self { source }
    }

    #[cfg(test)]
    pub(super) const fn record_error(&self) -> &RecordError {
        &self.source
    }
}

impl fmt::Display for ProduceMaterializationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Kafka record-batch materialization failed: {}",
            self.source
        )
    }
}

impl Error for ProduceMaterializationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
