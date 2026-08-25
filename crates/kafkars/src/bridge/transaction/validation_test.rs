//! Fresh transaction topic-description correlation scenarios.

use crate::{
    ErrorKind, KafkaError, TopicUuid,
    admin::{BatchResult, TopicDescription},
};

use super::{identity::TransactionIdentityState, validation::validate_topic_descriptions};

#[test]
fn exact_complete_topic_uuid_snapshot_validates() {
    let (state, uuid) = bound_state();
    let result = descriptions(vec![(
        "orders".to_owned(),
        Ok(description("orders", Some(uuid.into_bytes()))),
    )]);

    validate_topic_descriptions(&state, &result)
        .unwrap_or_else(|error| panic!("exact topic identity failed: {error}"));
    assert!(
        !state.is_sealed(),
        "checking broker evidence alone cannot install an observed validation seal"
    );
}

#[test]
fn missing_mismatched_and_uncorrelated_identities_fail_closed() {
    let (state, _uuid) = bound_state();
    for result in [
        descriptions(Vec::new()),
        descriptions(vec![(
            "orders".to_owned(),
            Ok(description("orders", Some(topic_uuid(2).into_bytes()))),
        )]),
        descriptions(vec![(
            "other".to_owned(),
            Ok(description("other", Some(topic_uuid(1).into_bytes()))),
        )]),
    ] {
        let error = validate_topic_descriptions(&state, &result)
            .err()
            .unwrap_or_else(|| panic!("invalid identity snapshot must reject"));
        assert_eq!(error.kind(), ErrorKind::Identity);
        assert!(error.requires_transaction_abort());
    }
}

#[test]
fn zero_topic_uuid_is_invalid_identity_evidence() {
    let (state, _uuid) = bound_state();
    let result = descriptions(vec![(
        "orders".to_owned(),
        Ok(description("orders", Some([0; 16]))),
    )]);

    let error = validate_topic_descriptions(&state, &result)
        .err()
        .unwrap_or_else(|| panic!("zero topic UUID must reject"));

    assert_eq!(error.kind(), ErrorKind::Identity);
    assert!(error.requires_transaction_abort());
}

#[test]
fn extra_topic_description_cannot_validate_the_expected_set() {
    let (state, uuid) = bound_state();
    let result = descriptions(vec![
        (
            "orders".to_owned(),
            Ok(description("orders", Some(uuid.into_bytes()))),
        ),
        (
            "extra".to_owned(),
            Ok(description("extra", Some(topic_uuid(2).into_bytes()))),
        ),
    ]);

    let error = validate_topic_descriptions(&state, &result)
        .err()
        .unwrap_or_else(|| panic!("extra topic evidence must reject"));

    assert_eq!(error.kind(), ErrorKind::Identity);
    assert!(error.requires_transaction_abort());
}

#[test]
fn duplicate_topic_description_cannot_substitute_for_an_expected_topic() {
    let mut state = TransactionIdentityState::new();
    bind_topic(&mut state, "orders", topic_uuid(1));
    bind_topic(&mut state, "payments", topic_uuid(2));
    let result = descriptions(vec![
        (
            "orders".to_owned(),
            Ok(description("orders", Some(topic_uuid(1).into_bytes()))),
        ),
        (
            "orders".to_owned(),
            Ok(description("orders", Some(topic_uuid(1).into_bytes()))),
        ),
    ]);

    let error = validate_topic_descriptions(&state, &result)
        .err()
        .unwrap_or_else(|| panic!("duplicate topic evidence must reject"));

    assert_eq!(error.kind(), ErrorKind::Identity);
    assert!(error.requires_transaction_abort());
}

#[test]
fn per_topic_failure_blocks_validation_without_inventing_identity_evidence() {
    let (state, _uuid) = bound_state();
    let result = descriptions(vec![(
        "orders".to_owned(),
        Err(KafkaError::new(
            ErrorKind::Broker,
            "DescribeTopics rejected orders",
        )),
    )]);

    let error = validate_topic_descriptions(&state, &result)
        .err()
        .unwrap_or_else(|| panic!("per-topic failure must block validation"));

    assert_eq!(error.kind(), ErrorKind::Broker);
    assert!(!error.requires_transaction_abort());
    assert!(!state.is_sealed());
}

fn bound_state() -> (TransactionIdentityState, TopicUuid) {
    let uuid = topic_uuid(1);
    let mut state = TransactionIdentityState::new();
    bind_topic(&mut state, "orders", uuid);
    (state, uuid)
}

fn bind_topic(state: &mut TransactionIdentityState, topic: &str, uuid: TopicUuid) {
    let prepared = state
        .prepare_mutation(Some((topic, Some(uuid))))
        .unwrap_or_else(|error| panic!("prepare binding failed: {error}"));
    state.commit_mutation(prepared);
}

fn descriptions(
    entries: Vec<(String, Result<TopicDescription, crate::KafkaError>)>,
) -> BatchResult<String, TopicDescription> {
    BatchResult::new(entries)
}

fn description(name: &str, topic_id: Option<[u8; 16]>) -> TopicDescription {
    TopicDescription::new(name.to_owned(), topic_id, false, Vec::new())
}

fn topic_uuid(last: u8) -> TopicUuid {
    let mut bytes = [0_u8; 16];
    bytes[15] = last;
    TopicUuid::try_from_bytes(bytes).unwrap_or_else(|| panic!("nonzero UUID"))
}
