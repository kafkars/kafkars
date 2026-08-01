//! Structural normalization of one explicit-partition generated Produce response.

use kafka_client_core::{DeliveryStatus, ProducerBatchSuccess, ProducerBrokerFailure};
use kafka_wire::ProduceResponse;

use super::produce_failure::normalize_produce_failure;

/// Why a driver-owned Produce response could not become an acknowledgment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProduceResponseFailure {
    /// Kafka returned a nonzero error code for the expected partition.
    Broker {
        /// Protocol-normalized broker policy fact.
        failure: ProducerBrokerFailure,
        /// Conservative certainty after driver ownership.
        delivery: DeliveryStatus,
    },
    /// The response could not be correlated to the one requested partition.
    Protocol {
        /// Exact structural mismatch.
        failure: ProduceResponseProtocolFailure,
        /// Conservative certainty after driver ownership.
        delivery: DeliveryStatus,
    },
}

impl ProduceResponseFailure {
    const fn broker(failure: ProducerBrokerFailure) -> Self {
        Self::Broker {
            failure,
            delivery: DeliveryStatus::PossiblySent,
        }
    }

    const fn protocol(failure: ProduceResponseProtocolFailure) -> Self {
        Self::Protocol {
            failure,
            delivery: DeliveryStatus::PossiblySent,
        }
    }

    /// Returns the authoritative post-submission delivery certainty.
    pub(crate) const fn delivery(self) -> DeliveryStatus {
        match self {
            Self::Broker { delivery, .. } | Self::Protocol { delivery, .. } => delivery,
        }
    }
}

/// Invalid response shapes for one name-routed explicit-partition Produce request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProduceResponseProtocolFailure {
    /// The broker returned zero or multiple topic responses.
    TopicCount {
        /// Observed number of topic responses.
        actual: usize,
    },
    /// The sole topic response did not name the requested topic.
    TopicNameMismatch,
    /// The broker returned zero or multiple partition responses.
    PartitionCount {
        /// Observed number of partition responses.
        actual: usize,
    },
    /// The sole partition response did not name the requested partition.
    PartitionIndexMismatch {
        /// Observed partition index.
        actual: i32,
    },
    /// A zero-code partition response still reported per-record failures.
    RecordErrorsOnSuccess {
        /// Observed number of per-record failures.
        actual: usize,
    },
    /// A zero-code partition response still carried a partition failure message.
    ErrorMessageOnSuccess,
    /// A successful response carried an impossible negative base offset.
    NegativeBaseOffset {
        /// Observed base offset.
        actual: i64,
    },
}

/// Normalizes one generated response for one expected topic and partition.
///
/// The driver already owns this request, so every failure is conservatively
/// `PossiblySent`. Generated protocol values do not cross the returned boundary.
pub(crate) fn normalize_explicit_produce_response(
    response: &ProduceResponse,
    expected_topic: &str,
    expected_partition: i32,
) -> Result<ProducerBatchSuccess, ProduceResponseFailure> {
    let [topic] = response.responses.as_slice() else {
        return Err(ProduceResponseFailure::protocol(
            ProduceResponseProtocolFailure::TopicCount {
                actual: response.responses.len(),
            },
        ));
    };
    if topic.name.as_str() != expected_topic {
        return Err(ProduceResponseFailure::protocol(
            ProduceResponseProtocolFailure::TopicNameMismatch,
        ));
    }

    let [partition] = topic.partition_responses.as_slice() else {
        return Err(ProduceResponseFailure::protocol(
            ProduceResponseProtocolFailure::PartitionCount {
                actual: topic.partition_responses.len(),
            },
        ));
    };
    if partition.index != expected_partition {
        return Err(ProduceResponseFailure::protocol(
            ProduceResponseProtocolFailure::PartitionIndexMismatch {
                actual: partition.index,
            },
        ));
    }

    if let Some(failure) = normalize_produce_failure(partition) {
        return Err(ProduceResponseFailure::broker(failure));
    }
    if !partition.record_errors.is_empty() {
        return Err(ProduceResponseFailure::protocol(
            ProduceResponseProtocolFailure::RecordErrorsOnSuccess {
                actual: partition.record_errors.len(),
            },
        ));
    }
    if partition.error_message.is_some() {
        return Err(ProduceResponseFailure::protocol(
            ProduceResponseProtocolFailure::ErrorMessageOnSuccess,
        ));
    }
    if partition.base_offset < 0 {
        return Err(ProduceResponseFailure::protocol(
            ProduceResponseProtocolFailure::NegativeBaseOffset {
                actual: partition.base_offset,
            },
        ));
    }

    Ok(ProducerBatchSuccess::new(
        partition.base_offset,
        nonnegative_i64(partition.log_append_time_ms),
        nonnegative_i32(partition.current_leader.leader_epoch),
    ))
}

const fn nonnegative_i64(value: i64) -> Option<i64> {
    if value < 0 { None } else { Some(value) }
}

const fn nonnegative_i32(value: i32) -> Option<i32> {
    if value < 0 { None } else { Some(value) }
}
