//! Errors rejected by deterministic producer-machine transitions.

use core::fmt;

use crate::{
    AdmissionRejection, BatchExecutionId, CapacityError, CompletionLedgerError, FlushLedgerError,
    TransitionError,
};

/// Rejected producer-machine transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerMachineError {
    /// Admission policy rejected otherwise valid explicit record facts.
    Admission(AdmissionRejection),
    /// The operation is not retained by this producer.
    UnknownOperation,
    /// The batch is not retained by this producer.
    UnknownBatch,
    /// The requested lifecycle transition is invalid.
    Transition(TransitionError),
    /// Terminal-completion ownership rejected the transition.
    Completion(CompletionLedgerError),
    /// Bounded flush-completion ownership rejected the transition.
    Flush(FlushLedgerError),
    /// Retained-byte accounting rejected the transition.
    Capacity(CapacityError),
    /// Conservative accumulator-size arithmetic could not be represented.
    AccumulatorSizeOverflow,
    /// A timer generation could not advance without reuse.
    TimerGenerationExhausted,
    /// A sealed execution generation could not advance without reuse.
    ExecutionGenerationExhausted,
    /// Broker returned an invalid producer ID or epoch.
    InvalidProducerIdentity,
    /// Identity fencing forbids assigning another partition sequence.
    ProducerIdentityFenced,
    /// A batch record count cannot be represented as one Kafka sequence range.
    SequenceRangeOverflow,
    /// Transport accepted bytes from a revoked or already-released execution.
    StaleDriverAcceptance {
        /// Exact execution reported by transport.
        reported: BatchExecutionId,
        /// Current execution retained by core, when the batch remains live.
        current: Option<BatchExecutionId>,
    },
    /// A broker base offset could not fan out across every record.
    OffsetOverflow,
}

impl fmt::Display for ProducerMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(reason) => write!(formatter, "producer admission rejected: {reason:?}"),
            Self::UnknownOperation => formatter.write_str("producer operation is unknown"),
            Self::UnknownBatch => formatter.write_str("producer batch is unknown"),
            Self::Transition(error) => error.fmt(formatter),
            Self::Completion(error) => error.fmt(formatter),
            Self::Flush(error) => error.fmt(formatter),
            Self::Capacity(error) => error.fmt(formatter),
            Self::AccumulatorSizeOverflow => {
                formatter.write_str("producer accumulator size overflow")
            }
            Self::TimerGenerationExhausted => {
                formatter.write_str("producer timer generation exhausted")
            }
            Self::ExecutionGenerationExhausted => {
                formatter.write_str("producer execution generation exhausted")
            }
            Self::InvalidProducerIdentity => {
                formatter.write_str("broker returned an invalid producer identity")
            }
            Self::ProducerIdentityFenced => formatter.write_str("producer identity is fenced"),
            Self::SequenceRangeOverflow => {
                formatter.write_str("producer sequence range cannot be represented")
            }
            Self::StaleDriverAcceptance { reported, .. } => write!(
                formatter,
                "driver accepted stale producer batch {} generation {}",
                reported.batch_id().get(),
                reported.generation().get()
            ),
            Self::OffsetOverflow => formatter.write_str("producer record offset overflow"),
        }
    }
}

impl std::error::Error for ProducerMachineError {}
