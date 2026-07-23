//! Deterministic interpreter failures that expose broken ownership contracts.

use core::fmt;

use kafka_client_core::{BatchId, ByteCount, OperationId, PayloadId, ProducerMachineError};

/// A core rejection or an inconsistent engine-effect contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimulationError {
    /// Deterministic producer policy rejected an input fact.
    Core(ProducerMachineError),
    /// The virtual engine already owns this payload identity.
    DuplicatePayload(PayloadId),
    /// An effect referenced payload bytes the virtual engine does not own.
    UnknownPayload(PayloadId),
    /// Core and engine disagreed about charged payload bytes.
    PayloadSizeMismatch {
        /// Bytes owned by the virtual engine.
        actual: ByteCount,
        /// Bytes named by the core effect.
        expected: ByteCount,
    },
    /// The virtual engine already owns this batch identity.
    DuplicateBatch(BatchId),
    /// An effect referenced a batch the virtual engine does not own.
    UnknownBatch(BatchId),
    /// A batch was materialized for a different operation.
    BatchOperationMismatch {
        /// Operation recorded by the virtual engine.
        actual: OperationId,
        /// Operation named by the core effect.
        expected: OperationId,
    },
    /// Completion publication preceded engine resource release.
    ResourceStillRetained(OperationId),
    /// Core emitted a second terminal outcome for one operation.
    DuplicateTerminal(OperationId),
    /// No engine-owned terminal result exists for this operation.
    UnknownTerminal(OperationId),
    /// Core reclamation was reported before the engine released its result.
    TerminalStillRetained(OperationId),
}

impl fmt::Display for SimulationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Core(error) => error.fmt(formatter),
            Self::DuplicatePayload(_) => formatter.write_str("duplicate virtual payload"),
            Self::UnknownPayload(_) => formatter.write_str("unknown virtual payload"),
            Self::PayloadSizeMismatch { .. } => {
                formatter.write_str("virtual payload byte count does not match")
            }
            Self::DuplicateBatch(_) => formatter.write_str("duplicate virtual batch"),
            Self::UnknownBatch(_) => formatter.write_str("unknown virtual batch"),
            Self::BatchOperationMismatch { .. } => {
                formatter.write_str("virtual batch belongs to a different operation")
            }
            Self::ResourceStillRetained(_) => {
                formatter.write_str("completion preceded virtual resource release")
            }
            Self::DuplicateTerminal(_) => formatter.write_str("duplicate virtual terminal result"),
            Self::UnknownTerminal(_) => formatter.write_str("unknown virtual terminal result"),
            Self::TerminalStillRetained(_) => {
                formatter.write_str("terminal result remains retained by the virtual engine")
            }
        }
    }
}

impl std::error::Error for SimulationError {}
