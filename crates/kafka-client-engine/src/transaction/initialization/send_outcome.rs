//! Stable public outcomes for one accepted transactional record send.

use std::sync::Arc;

use kafka_client_core::{ProducerBatchSuccess, TransactionEpoch, TransactionSendId};

use crate::transaction::send::TransactionSendTerminal as InternalTerminal;

use super::send_failure_mapping::public_failure;

/// Authoritative transport certainty for a failed transactional send.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionSendDeliveryStatus {
    /// The record definitely did not enter transport ownership.
    NotSent,
    /// Kafka may have observed the record.
    PossiblySent,
}

/// Deterministic effect of a failed send on the active transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionSendConsequence {
    /// This send failed, but the transaction remains eligible to commit.
    FailedHealthy,
    /// The transaction may now only be aborted.
    AbortRequired,
    /// The initialized producer owner is permanently unusable.
    Fatal,
}

/// Stable reason one accepted transactional send failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionSendFailureKind {
    /// Another bounded transactional send owns the sole slot.
    Busy,
    /// The active transaction no longer matches the send.
    StaleTransaction,
    /// The initialized transactional owner is unavailable.
    OwnerUnavailable,
    /// Topic or partition routing was invalid.
    InvalidTarget,
    /// A fixed record, topic, partition, or driver capacity was full.
    Backpressure,
    /// The original public absolute deadline elapsed.
    DeadlineElapsed,
    /// Driver admission rejected before transport ownership.
    DriverRejected,
    /// Driver-owned transport execution failed.
    Transport,
    /// The negotiated protocol or record format was incompatible.
    Compatibility,
    /// Broker response shape or correlation was invalid.
    InvalidResponse,
    /// The driver or its completion owner closed.
    DriverClosed,
    /// Kafka returned an exact signed broker rejection.
    Broker,
    /// Authentication or authorization rejected the operation.
    Access,
    /// The transaction coordinator could not serve enrollment.
    Coordinator,
    /// Kafka explicitly fenced the transactional producer identity.
    Fenced,
    /// Kafka rejected record or batch content.
    InvalidRecord,
    /// Broker-issued topic identity did not match the caller expectation.
    Identity,
    /// Idempotent producer identity or sequence state became unusable.
    ProducerIdentity,
    /// Record-batch materialization failed before Produce ownership.
    Materialization,
    /// No usable broker route was available.
    Routing,
    /// A required broker name could not be resolved.
    NameResolution,
    /// A usable connection generation was unavailable.
    ConnectionUnavailable,
    /// A structural failure cannot be repaired by retry.
    Permanent,
    /// Internal epoch or send correlation failed.
    Correlation,
}

/// Exact failure, delivery certainty, and transaction consequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionSendFailure {
    pub(super) kind: TransactionSendFailureKind,
    pub(super) delivery: TransactionSendDeliveryStatus,
    pub(super) broker_code: Option<i16>,
    pub(super) consequence: TransactionSendConsequence,
}

impl TransactionSendFailure {
    /// Returns the stable failure category.
    pub const fn kind(self) -> TransactionSendFailureKind {
        self.kind
    }

    /// Returns authoritative transport certainty.
    pub const fn delivery(self) -> TransactionSendDeliveryStatus {
        self.delivery
    }

    /// Returns Kafka's exact signed broker code when present.
    pub const fn broker_code(self) -> Option<i16> {
        self.broker_code
    }

    /// Returns this send failure's deterministic transaction consequence.
    pub const fn consequence(self) -> TransactionSendConsequence {
        self.consequence
    }
}

/// Kafka acknowledgment metadata for one transactional record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionSendMetadata {
    topic: Arc<str>,
    partition: i32,
    offset: i64,
    last_offset: i64,
    timestamp: Option<i64>,
    leader_epoch: Option<i32>,
}

impl TransactionSendMetadata {
    /// Returns the exact canonical topic spelling admitted for this send.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the acknowledged zero-based partition.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the record's absolute Kafka offset.
    pub const fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the acknowledged offset of the last record in this send.
    pub const fn last_offset(&self) -> i64 {
        self.last_offset
    }

    /// Returns Kafka's append timestamp when supplied.
    pub const fn timestamp(&self) -> Option<i64> {
        self.timestamp
    }

    /// Returns Kafka's leader epoch when supplied.
    pub const fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }
}

/// Exactly one public terminal for an accepted transactional record send.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransactionSendOutcome {
    /// Kafka acknowledged the record.
    Succeeded(TransactionSendMetadata),
    /// The record failed with an explicit transaction consequence.
    Failed(TransactionSendFailure),
}

pub(super) fn translate_send_terminal(
    terminal: InternalTerminal,
    epoch: TransactionEpoch,
    send_id: TransactionSendId,
    topic: Arc<str>,
    partition: Option<i32>,
) -> Option<TransactionSendOutcome> {
    match terminal {
        InternalTerminal::Succeeded {
            epoch: actual_epoch,
            send_id: actual_send_id,
            partition: actual_partition,
            success,
            last_offset,
        } if actual_epoch == epoch
            && actual_send_id == send_id
            && partition.is_none_or(|partition| {
                u32::try_from(partition).ok() == Some(actual_partition.get())
            }) =>
        {
            let partition = i32::try_from(actual_partition.get()).ok()?;
            Some(TransactionSendOutcome::Succeeded(success_metadata(
                success,
                last_offset,
                topic,
                partition,
            )))
        }
        InternalTerminal::FailedHealthy {
            epoch: actual_epoch,
            send_id: actual_send_id,
            failure,
        } if actual_epoch == epoch && actual_send_id == send_id => Some(public_failure(
            failure,
            TransactionSendConsequence::FailedHealthy,
        )),
        InternalTerminal::AbortRequired {
            epoch: actual_epoch,
            send_id: actual_send_id,
            failure,
        } if actual_epoch == epoch && actual_send_id == send_id => Some(public_failure(
            failure,
            TransactionSendConsequence::AbortRequired,
        )),
        InternalTerminal::Fatal {
            epoch: actual_epoch,
            send_id: actual_send_id,
            failure,
        } if actual_epoch == epoch && actual_send_id == send_id => {
            Some(public_failure(failure, TransactionSendConsequence::Fatal))
        }
        _ => None,
    }
}

fn success_metadata(
    success: ProducerBatchSuccess,
    last_offset: i64,
    topic: Arc<str>,
    partition: i32,
) -> TransactionSendMetadata {
    TransactionSendMetadata {
        topic,
        partition,
        offset: success.base_offset(),
        last_offset,
        timestamp: success.append_timestamp(),
        leader_epoch: success.leader_epoch(),
    }
}
