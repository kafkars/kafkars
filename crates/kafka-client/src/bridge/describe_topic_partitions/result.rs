//! Exhaustive stable translation of engine Admin `DescribeTopicPartitions` outcomes.

use std::time::Duration;

use crate::{
    DeliveryStatus as PublicDeliveryStatus, ErrorKind, KafkaError,
    admin::{
        DescribeTopicPartition as PublicPartition, DescribeTopicPartitionsCursor as PublicCursor,
        DescribeTopicPartitionsPage as PublicPage, DescribeTopicPartitionsTopic as PublicTopic,
    },
};

use super::{
    engine::{
        AcceptedFaultKind, AdmissionError, AdmissionErrorKind, DeliveryStatus, Failure,
        FailureKind, ObserverError, Outcome, Page, Partition, Topic,
    },
    operation::AdminDescribeTopicPartitionsResult,
};

pub(super) fn translate_admission_error(error: AdmissionError) -> KafkaError {
    translate_admission_kind(error.kind())
}

pub(super) fn translate_admission_kind(kind: AdmissionErrorKind) -> KafkaError {
    let public = match kind {
        AdmissionErrorKind::InvalidRequest | AdmissionErrorKind::InvalidDeadline => {
            ErrorKind::Configuration
        }
        AdmissionErrorKind::Contended
        | AdmissionErrorKind::Capacity
        | AdmissionErrorKind::RetainedBytes => ErrorKind::Backpressure,
        AdmissionErrorKind::Closed => ErrorKind::State,
        AdmissionErrorKind::IdentityExhausted | AdmissionErrorKind::HostUnavailable => {
            ErrorKind::Internal
        }
    };
    KafkaError::new(
        public,
        format!("DescribeTopicPartitions admission failed: {kind:?}"),
    )
    .with_delivery_status(PublicDeliveryStatus::NotSent)
}

pub(super) fn translate_accepted_fault(fault: AcceptedFaultKind) -> KafkaError {
    match fault {
        AcceptedFaultKind::Wake => KafkaError::new(
            ErrorKind::Internal,
            "DescribeTopicPartitions was accepted but its host wake failed",
        ),
        AcceptedFaultKind::HostInvariant => KafkaError::new(
            ErrorKind::Internal,
            "DescribeTopicPartitions was accepted but its host reported an invariant failure",
        ),
    }
}

pub(super) fn translate_observation(
    result: Result<Outcome, ObserverError>,
) -> AdminDescribeTopicPartitionsResult {
    match result {
        Ok(Outcome::Page(page)) => Ok(translate_page(page)),
        Ok(Outcome::Failed(failure)) => Err(translate_failure(failure)),
        Err(error) => Err(translate_observer_error(error)),
    }
}

fn translate_page(page: Page) -> PublicPage {
    let (throttle_time_ms, topics, cursor) = page.into_parts();
    page_from_parts(
        throttle_time_ms,
        topics.into_iter().map(translate_topic).collect(),
        cursor.map(|cursor| cursor.into_parts()),
    )
}

fn translate_topic(topic: Topic) -> PublicTopic {
    let (code, name, topic_id, internal, partitions, authorized_operations) = topic.into_parts();
    topic_from_parts(
        code,
        name,
        topic_id,
        internal,
        partitions.into_iter().map(translate_partition).collect(),
        authorized_operations,
    )
}

fn translate_partition(partition: Partition) -> PublicPartition {
    let (
        code,
        partition_index,
        leader_id,
        leader_epoch,
        replicas,
        isr,
        eligible_leader_replicas,
        last_known_elr,
        offline_replicas,
    ) = partition.into_parts();
    partition_from_parts(
        code,
        partition_index,
        leader_id,
        leader_epoch,
        replicas,
        isr,
        eligible_leader_replicas,
        last_known_elr,
        offline_replicas,
    )
}

pub(super) fn page_from_parts(
    throttle_time_ms: u32,
    topics: Vec<PublicTopic>,
    cursor: Option<(String, i32)>,
) -> PublicPage {
    PublicPage::new(
        Duration::from_millis(u64::from(throttle_time_ms)),
        topics,
        cursor.map(|(topic, partition)| PublicCursor::new(topic, partition)),
    )
}

pub(super) fn topic_from_parts(
    code: i16,
    name: String,
    topic_id: [u8; 16],
    internal: bool,
    partitions: Vec<PublicPartition>,
    authorized_operations: i32,
) -> PublicTopic {
    PublicTopic::new(
        broker_error(code, "topic").map(|error| error.with_internal_topic(internal)),
        name,
        topic_id,
        internal,
        partitions,
        authorized_operations,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn partition_from_parts(
    code: i16,
    partition_index: i32,
    leader_id: Option<i32>,
    leader_epoch: Option<i32>,
    replicas: Vec<i32>,
    isr: Vec<i32>,
    eligible_leader_replicas: Option<Vec<i32>>,
    last_known_elr: Option<Vec<i32>>,
    offline_replicas: Vec<i32>,
) -> PublicPartition {
    PublicPartition::new(
        broker_error(code, "partition"),
        partition_index,
        leader_id,
        leader_epoch,
        replicas,
        isr,
        eligible_leader_replicas,
        last_known_elr,
        offline_replicas,
    )
}

pub(super) fn broker_error(code: i16, scope: &str) -> Option<KafkaError> {
    (code != 0).then(|| {
        KafkaError::new(
            ErrorKind::Broker,
            format!("Kafka returned DescribeTopicPartitions {scope} code {code}"),
        )
        .with_broker_code(Some(code))
        .with_delivery_status(PublicDeliveryStatus::PossiblySent)
    })
}

fn translate_failure(failure: Failure) -> KafkaError {
    translate_failure_parts(failure.kind(), failure.delivery())
}

pub(super) fn translate_failure_parts(kind: FailureKind, delivery: DeliveryStatus) -> KafkaError {
    let public = match kind {
        FailureKind::DeadlineElapsed => ErrorKind::Timeout,
        FailureKind::DriverRejected | FailureKind::ResponseTooLarge => ErrorKind::Backpressure,
        FailureKind::Transport => ErrorKind::Transport,
        FailureKind::Compatibility => ErrorKind::Compatibility,
        FailureKind::InvalidResponse => ErrorKind::Broker,
    };
    KafkaError::new(public, format!("DescribeTopicPartitions failed: {kind:?}"))
        .with_delivery_status(translate_delivery(delivery))
}

const fn translate_delivery(delivery: DeliveryStatus) -> PublicDeliveryStatus {
    match delivery {
        DeliveryStatus::NotSent => PublicDeliveryStatus::NotSent,
        DeliveryStatus::PossiblySent => PublicDeliveryStatus::PossiblySent,
    }
}

pub(super) fn translate_observer_error(error: ObserverError) -> KafkaError {
    let public = match error {
        ObserverError::AlreadyObserved => ErrorKind::State,
        ObserverError::Stale => ErrorKind::Internal,
    };
    KafkaError::new(public, error.to_string())
}
