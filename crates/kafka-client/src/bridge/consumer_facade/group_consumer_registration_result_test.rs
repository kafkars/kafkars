//! Hosted group registration result translation shape contract.

use kafka_client_engine::{GroupConsumerRegistrationErrorKind, GroupConsumerStartError};

use super::group_consumer_registration_result::{
    translate_group_registration_kind, translate_group_start,
};
use crate::{ErrorKind, KafkaError};

#[test]
fn every_engine_start_error_crosses_one_translation_function() {
    fn contract(error: GroupConsumerStartError) -> KafkaError {
        translate_group_start(error)
    }
    let _ = contract as fn(GroupConsumerStartError) -> KafkaError;
}

#[test]
fn contention_and_capacity_remain_distinct_backpressure_diagnostics() {
    let contended =
        translate_group_registration_kind(GroupConsumerRegistrationErrorKind::Contended);
    let capacity =
        translate_group_registration_kind(GroupConsumerRegistrationErrorKind::Backpressure);
    assert_eq!(contended.kind(), ErrorKind::Backpressure);
    assert_eq!(capacity.kind(), ErrorKind::Backpressure);
    assert!(contended.to_string().contains("temporarily contended"));
    assert!(capacity.to_string().contains("capacity is full"));
}
