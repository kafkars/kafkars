//! Public-facade scenarios for partial Admin `DeleteRecords` terminals.

use crate::{
    DeliveryStatus, ErrorKind, KafkaError, TopicPartition, admin::DeleteRecordsResultInfo,
};

use super::result::partial_result;

#[test]
fn completed_transport_failure_and_unattempted_targets_remain_ordered() {
    let result = partial_result(
        13,
        vec![(target("a", 0), Ok(DeleteRecordsResultInfo::new(41)))],
        target("b", 1),
        KafkaError::new(ErrorKind::Transport, "transport failed")
            .with_delivery_status(DeliveryStatus::PossiblySent),
        vec![target("c", 2)],
    );
    let entries = result.records().entries();
    assert_eq!(result.throttle_time().as_millis(), 13);
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].0, target("a", 0));
    assert_eq!(
        entries[0]
            .1
            .as_ref()
            .unwrap_or_else(|error| panic!("completed target: {error}"))
            .low_watermark(),
        41
    );
    assert_eq!(entries[1].0, target("b", 1));
    let failed = entries[1]
        .1
        .as_ref()
        .err()
        .unwrap_or_else(|| panic!("failed target"));
    assert_eq!(failed.kind(), ErrorKind::Transport);
    assert_eq!(failed.delivery_status(), Some(DeliveryStatus::PossiblySent));
    assert_eq!(entries[2].0, target("c", 2));
    let unattempted = entries[2]
        .1
        .as_ref()
        .err()
        .unwrap_or_else(|| panic!("unattempted target"));
    assert_eq!(unattempted.kind(), ErrorKind::State);
    assert_eq!(unattempted.delivery_status(), Some(DeliveryStatus::NotSent));
}

#[test]
fn completed_deadline_failure_keeps_exact_not_sent_status() {
    let result = partial_result(
        7,
        vec![(target("a", 0), Ok(DeleteRecordsResultInfo::new(9)))],
        target("b", 1),
        KafkaError::new(ErrorKind::Timeout, "deadline elapsed")
            .with_delivery_status(DeliveryStatus::NotSent),
        Vec::new(),
    );
    let failure = result.records().entries()[1]
        .1
        .as_ref()
        .err()
        .unwrap_or_else(|| panic!("deadline failure"));
    assert_eq!(failure.kind(), ErrorKind::Timeout);
    assert_eq!(failure.delivery_status(), Some(DeliveryStatus::NotSent));
}

fn target(topic: &str, partition: i32) -> TopicPartition {
    TopicPartition::new(topic.to_owned(), partition)
}
