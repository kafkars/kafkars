//! Deterministic producer, consumer, transaction, and admin policy.

#![forbid(unsafe_code)]

mod admission;
mod capacity;
mod completion;
mod operation;
mod operation_outcome;
mod producer;
mod producer_broker_failure;
mod producer_effect;
mod producer_error;
mod producer_failure;
mod producer_input;
mod producer_policy;
mod producer_record;
mod producer_transition;
mod types;

pub use admission::AdmissionRejection;
pub use capacity::{ByteBudget, CapacityError};
pub use completion::{CompletionLedger, CompletionLedgerError};
pub use operation::{ProducerOperation, ProducerOperationState};
pub use operation_outcome::{
    DeliveryStatus, ProducerBatchSuccess, ProducerCompletion, RecordMetadata, TerminalRelease,
    TransitionError,
};
pub use producer::{
    AdmissionSequence, FlushId, FlushLedgerError, KeyedPartitionError, PartitionCount,
    ProducerMachine, select_java_keyed_partition,
};
pub use producer_broker_failure::{ProducerBrokerFailure, ProducerBrokerFailureKind};
pub use producer_effect::{
    AcknowledgementPolicy, CompressionPolicy, EXECUTION_STOP_EFFECTS_PER_FLUSH,
    EXECUTION_STOP_EFFECTS_PER_RECORD, ProducerEffect, ProducerTransition,
    execution_stop_effect_capacity, producer_transition_effect_capacity,
};
pub use producer_error::ProducerMachineError;
pub use producer_failure::{ProducerFailure, ProducerFailureKind};
pub use producer_input::ProducerInput;
pub use producer_policy::{ProducerBatchPolicy, ProducerBatchPolicyError};
pub use producer_record::{
    BatchExecutionGeneration, BatchExecutionId, BatchId, BatchTimerGeneration, ExplicitRecord,
    PartitionIndex, PayloadId, TopicId,
};
pub use types::{ByteCount, Deadline, Moment, OperationId};

#[cfg(test)]
mod capacity_test;
#[cfg(test)]
mod completion_test;
#[cfg(test)]
mod operation_test;
#[cfg(test)]
mod producer_broker_failure_test;
#[cfg(test)]
mod producer_failure_test;
#[cfg(test)]
mod producer_outcome_test;
#[cfg(test)]
mod producer_reclaim_test;
#[cfg(test)]
mod producer_submission_deadline_test;
#[cfg(test)]
mod producer_test;
#[cfg(test)]
mod producer_timer_test;
#[cfg(test)]
mod producer_transition_identity_test;
#[cfg(test)]
mod producer_transition_test;
