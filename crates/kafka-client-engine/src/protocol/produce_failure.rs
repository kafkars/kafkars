//! Produce response error-code normalization at the generated protocol boundary.

use core::num::NonZeroI16;

use kafka_client_core::{ProducerBrokerFailure, ProducerBrokerFailureKind};
use kafka_wire::produce_response::PartitionProduceResponse;

use self::broker_code::{
    CORRUPT_MESSAGE, DUPLICATE_SEQUENCE_NUMBER, FENCED_LEADER_EPOCH, INCONSISTENT_TOPIC_ID,
    INVALID_PRODUCER_EPOCH, INVALID_RECORD, INVALID_REQUEST, INVALID_REQUIRED_ACKS,
    INVALID_TIMESTAMP, INVALID_TOPIC_EXCEPTION, KAFKA_STORAGE_ERROR, LEADER_NOT_AVAILABLE,
    MESSAGE_TOO_LARGE, NETWORK_EXCEPTION, NOT_ENOUGH_REPLICAS, NOT_ENOUGH_REPLICAS_AFTER_APPEND,
    NOT_LEADER_OR_FOLLOWER, OUT_OF_ORDER_SEQUENCE_NUMBER, POLICY_VIOLATION, PRODUCER_FENCED,
    RECORD_LIST_TOO_LARGE, REQUEST_TIMED_OUT, SASL_AUTHENTICATION_FAILED,
    THROTTLING_QUOTA_EXCEEDED, TOPIC_AUTHORIZATION_FAILED, UNKNOWN_LEADER_EPOCH,
    UNKNOWN_PRODUCER_ID, UNKNOWN_TOPIC_ID, UNKNOWN_TOPIC_OR_PARTITION,
    UNSUPPORTED_COMPRESSION_TYPE, UNSUPPORTED_FOR_MESSAGE_FORMAT, UNSUPPORTED_VERSION,
};

/// Converts one generated partition response into a semantic broker failure.
pub(crate) fn normalize_produce_failure(
    response: &PartitionProduceResponse,
) -> Option<ProducerBrokerFailure> {
    let code = NonZeroI16::new(response.error_code)?;
    Some(ProducerBrokerFailure::new(classify(code.get()), code))
}

const fn classify(code: i16) -> ProducerBrokerFailureKind {
    match code {
        UNKNOWN_TOPIC_OR_PARTITION
        | LEADER_NOT_AVAILABLE
        | NOT_LEADER_OR_FOLLOWER
        | FENCED_LEADER_EPOCH
        | UNKNOWN_LEADER_EPOCH
        | UNKNOWN_TOPIC_ID
        | INCONSISTENT_TOPIC_ID => ProducerBrokerFailureKind::Routing,
        CORRUPT_MESSAGE
        | REQUEST_TIMED_OUT
        | NETWORK_EXCEPTION
        | NOT_ENOUGH_REPLICAS
        | NOT_ENOUGH_REPLICAS_AFTER_APPEND
        | KAFKA_STORAGE_ERROR
        | THROTTLING_QUOTA_EXCEEDED => ProducerBrokerFailureKind::Retriable,
        TOPIC_AUTHORIZATION_FAILED | SASL_AUTHENTICATION_FAILED => {
            ProducerBrokerFailureKind::AccessRejected
        }
        MESSAGE_TOO_LARGE
        | INVALID_TOPIC_EXCEPTION
        | RECORD_LIST_TOO_LARGE
        | INVALID_REQUIRED_ACKS
        | INVALID_TIMESTAMP
        | INVALID_REQUEST
        | POLICY_VIOLATION
        | INVALID_RECORD => ProducerBrokerFailureKind::InvalidRecord,
        UNSUPPORTED_VERSION | UNSUPPORTED_FOR_MESSAGE_FORMAT | UNSUPPORTED_COMPRESSION_TYPE => {
            ProducerBrokerFailureKind::Compatibility
        }
        OUT_OF_ORDER_SEQUENCE_NUMBER
        | DUPLICATE_SEQUENCE_NUMBER
        | INVALID_PRODUCER_EPOCH
        | UNKNOWN_PRODUCER_ID => ProducerBrokerFailureKind::ProducerIdentity,
        PRODUCER_FENCED => ProducerBrokerFailureKind::ProducerFenced,
        _ => ProducerBrokerFailureKind::Unknown,
    }
}

mod broker_code {
    pub(super) const CORRUPT_MESSAGE: i16 = 2;
    pub(super) const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
    pub(super) const LEADER_NOT_AVAILABLE: i16 = 5;
    pub(super) const NOT_LEADER_OR_FOLLOWER: i16 = 6;
    pub(super) const REQUEST_TIMED_OUT: i16 = 7;
    pub(super) const MESSAGE_TOO_LARGE: i16 = 10;
    pub(super) const NETWORK_EXCEPTION: i16 = 13;
    pub(super) const INVALID_TOPIC_EXCEPTION: i16 = 17;
    pub(super) const RECORD_LIST_TOO_LARGE: i16 = 18;
    pub(super) const NOT_ENOUGH_REPLICAS: i16 = 19;
    pub(super) const NOT_ENOUGH_REPLICAS_AFTER_APPEND: i16 = 20;
    pub(super) const INVALID_REQUIRED_ACKS: i16 = 21;
    pub(super) const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
    pub(super) const INVALID_TIMESTAMP: i16 = 32;
    pub(super) const UNSUPPORTED_VERSION: i16 = 35;
    pub(super) const INVALID_REQUEST: i16 = 42;
    pub(super) const UNSUPPORTED_FOR_MESSAGE_FORMAT: i16 = 43;
    pub(super) const POLICY_VIOLATION: i16 = 44;
    pub(super) const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
    pub(super) const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
    pub(super) const INVALID_PRODUCER_EPOCH: i16 = 47;
    pub(super) const KAFKA_STORAGE_ERROR: i16 = 56;
    pub(super) const SASL_AUTHENTICATION_FAILED: i16 = 58;
    pub(super) const UNKNOWN_PRODUCER_ID: i16 = 59;
    pub(super) const FENCED_LEADER_EPOCH: i16 = 74;
    pub(super) const UNKNOWN_LEADER_EPOCH: i16 = 75;
    pub(super) const UNSUPPORTED_COMPRESSION_TYPE: i16 = 76;
    pub(super) const INVALID_RECORD: i16 = 87;
    pub(super) const THROTTLING_QUOTA_EXCEEDED: i16 = 89;
    pub(super) const PRODUCER_FENCED: i16 = 90;
    pub(super) const UNKNOWN_TOPIC_ID: i16 = 100;
    pub(super) const INCONSISTENT_TOPIC_ID: i16 = 103;
}
