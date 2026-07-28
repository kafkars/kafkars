//! Public-facade scenarios for partial Admin `DeleteConsumerGroups` terminals.

use crate::{DeliveryStatus, ErrorKind, KafkaError};

use super::result::{partial_result, translate_group_error};

#[test]
fn group_broker_diagnostic_and_truncation_reach_public_error() {
    let error = translate_group_error((
        -31_997,
        Some("coordinator rejected deletion".to_owned()),
        true,
    ));
    assert_eq!(error.kind(), ErrorKind::Broker);
    assert_eq!(error.broker_code(), Some(-31_997));
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert_eq!(
        error.to_string(),
        "Kafka returned DeleteConsumerGroups group broker code -31997: coordinator rejected deletion"
    );
    assert!(error.diagnostic_truncated());
}

#[test]
fn completed_transport_failure_and_unattempted_groups_remain_ordered() {
    let result = partial_result(
        13,
        vec![("a".to_owned(), Ok(()))],
        "b".to_owned(),
        KafkaError::new(ErrorKind::Transport, "transport failed")
            .with_delivery_status(DeliveryStatus::PossiblySent),
        vec!["c".to_owned()],
    );
    let entries = result.groups().entries();
    assert_eq!(result.throttle_time().as_millis(), 13);
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].0, "a");
    assert!(entries[0].1.is_ok());
    assert_eq!(entries[1].0, "b");
    let failed = entries[1]
        .1
        .as_ref()
        .err()
        .unwrap_or_else(|| panic!("failed group"));
    assert_eq!(failed.kind(), ErrorKind::Transport);
    assert_eq!(failed.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert_eq!(entries[2].0, "c");
    let unattempted = entries[2]
        .1
        .as_ref()
        .err()
        .unwrap_or_else(|| panic!("unattempted group"));
    assert_eq!(unattempted.kind(), ErrorKind::State);
    assert_eq!(unattempted.delivery_status(), Some(DeliveryStatus::NotSent));
}

#[test]
fn completed_deadline_failure_keeps_exact_not_sent_status() {
    let result = partial_result(
        7,
        vec![("a".to_owned(), Ok(()))],
        "b".to_owned(),
        KafkaError::new(ErrorKind::Timeout, "deadline elapsed")
            .with_delivery_status(DeliveryStatus::NotSent),
        Vec::new(),
    );
    let failure = result.groups().entries()[1]
        .1
        .as_ref()
        .err()
        .unwrap_or_else(|| panic!("deadline failure"));
    assert_eq!(failure.kind(), ErrorKind::Timeout);
    assert_eq!(failure.delivery_status(), Some(DeliveryStatus::NotSent));
}
