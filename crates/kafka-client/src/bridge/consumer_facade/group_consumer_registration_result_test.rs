//! Hosted group registration result translation shape contract.

use kafka_client_engine::{
    GroupConsumerMissingOffsetPolicy, GroupConsumerRegistrationErrorKind, GroupConsumerStartError,
};

use super::group_consumer_registration::engine_missing_offset_policy;
use super::group_consumer_registration_result::{
    translate_group_registration_kind, translate_group_start,
};
use crate::{ErrorKind, KafkaError, OffsetReset};

#[test]
fn every_engine_start_error_crosses_one_translation_function() {
    fn contract(error: GroupConsumerStartError) -> KafkaError {
        translate_group_start(error)
    }
    let _ = contract as fn(GroupConsumerStartError) -> KafkaError;
}

#[test]
fn every_live_registration_category_crosses_without_a_placeholder_protocol_error() {
    for (kind, expected) in [
        (GroupConsumerRegistrationErrorKind::Closed, ErrorKind::State),
        (
            GroupConsumerRegistrationErrorKind::Contended,
            ErrorKind::Backpressure,
        ),
        (
            GroupConsumerRegistrationErrorKind::Backpressure,
            ErrorKind::Backpressure,
        ),
        (
            GroupConsumerRegistrationErrorKind::InvalidInput,
            ErrorKind::Configuration,
        ),
        (
            GroupConsumerRegistrationErrorKind::Internal,
            ErrorKind::Internal,
        ),
    ] {
        assert_eq!(translate_group_registration_kind(kind).kind(), expected);
    }

    let contended =
        translate_group_registration_kind(GroupConsumerRegistrationErrorKind::Contended);
    let capacity =
        translate_group_registration_kind(GroupConsumerRegistrationErrorKind::Backpressure);
    assert!(contended.to_string().contains("temporarily contended"));
    assert!(capacity.to_string().contains("capacity is full"));
}

#[test]
fn every_missing_offset_choice_crosses_the_registration_boundary_exactly() {
    for (policy, expected) in [
        (OffsetReset::Error, GroupConsumerMissingOffsetPolicy::Error),
        (
            OffsetReset::Earliest,
            GroupConsumerMissingOffsetPolicy::Earliest,
        ),
        (
            OffsetReset::Latest,
            GroupConsumerMissingOffsetPolicy::Latest,
        ),
    ] {
        assert_eq!(engine_missing_offset_policy(policy), expected);
    }
}
