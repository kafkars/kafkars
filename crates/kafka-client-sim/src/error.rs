//! Deterministic interpreter failures that expose broken ownership contracts.

use core::fmt;

use kafka_client_core::{
    BatchExecutionId, BatchId, ByteCount, OperationId, PayloadId, ProducerMachineError,
};

use crate::VirtualClockError;

/// A core rejection or an inconsistent engine-effect contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimulationError {
    /// The requested virtual-time target exceeded the monotonic domain.
    Time(VirtualClockError),
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
    /// An effect referenced a batch the virtual engine does not own.
    UnknownBatch(BatchId),
    /// An operation was accumulated more than once.
    DuplicateOperation(OperationId),
    /// Accumulator membership changed after the execution was sealed.
    BatchMembershipClosed(BatchId),
    /// An effect named an operation outside its claimed accumulator.
    OperationNotInBatch(OperationId),
    /// Submission preceded the core-requested materialization mechanism.
    BatchNotMaterialized(BatchExecutionId),
    /// A virtual mechanism named a non-current execution generation.
    BatchExecutionMismatch {
        /// Execution retained by the virtual batch, when one exists.
        expected: Option<BatchExecutionId>,
        /// Execution named by the effect.
        actual: BatchExecutionId,
    },
    /// A surviving sealed batch revision omitted its replacement generation.
    MissingReplacementExecution(BatchExecutionId),
    /// Core attempted to revise a driver-owned batch execution.
    BatchExecutionAlreadySubmitted(BatchExecutionId),
    /// One virtual execution was materialized or submitted twice.
    DuplicateBatchExecution(BatchExecutionId),
    /// Completion publication preceded engine resource release.
    ResourceStillRetained(OperationId),
    /// Core emitted a second terminal outcome for one operation.
    DuplicateTerminal(OperationId),
    /// No engine-owned terminal result exists for this operation.
    UnknownTerminal(OperationId),
    /// Core reclamation was reported before the engine released its result.
    TerminalStillRetained(OperationId),
    /// Flush effects require completion ownership the simulator does not model.
    FlushControlUnavailable,
}

impl fmt::Display for SimulationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Time(error) => error.fmt(formatter),
            Self::Core(error) => error.fmt(formatter),
            Self::DuplicatePayload(_) => formatter.write_str("duplicate virtual payload"),
            Self::UnknownPayload(_) => formatter.write_str("unknown virtual payload"),
            Self::PayloadSizeMismatch { .. } => {
                formatter.write_str("virtual payload byte count does not match")
            }
            Self::UnknownBatch(_) => formatter.write_str("unknown virtual batch"),
            Self::DuplicateOperation(_) => formatter.write_str("duplicate virtual operation"),
            Self::BatchMembershipClosed(_) => {
                formatter.write_str("virtual batch membership is sealed")
            }
            Self::OperationNotInBatch(_) => {
                formatter.write_str("operation is not retained by the virtual batch")
            }
            Self::BatchNotMaterialized(_) => {
                formatter.write_str("virtual batch was not materialized")
            }
            Self::BatchExecutionMismatch { .. } => {
                formatter.write_str("virtual batch execution identity does not match")
            }
            Self::MissingReplacementExecution(_) => {
                formatter.write_str("virtual batch revision omitted its replacement")
            }
            Self::BatchExecutionAlreadySubmitted(_) => {
                formatter.write_str("virtual batch execution is already driver-owned")
            }
            Self::DuplicateBatchExecution(_) => {
                formatter.write_str("virtual batch execution was repeated")
            }
            Self::ResourceStillRetained(_) => {
                formatter.write_str("completion preceded virtual resource release")
            }
            Self::DuplicateTerminal(_) => formatter.write_str("duplicate virtual terminal result"),
            Self::UnknownTerminal(_) => formatter.write_str("unknown virtual terminal result"),
            Self::TerminalStillRetained(_) => {
                formatter.write_str("terminal result remains retained by the virtual engine")
            }
            Self::FlushControlUnavailable => {
                formatter.write_str("virtual producer flush completion is not implemented")
            }
        }
    }
}

impl std::error::Error for SimulationError {}
