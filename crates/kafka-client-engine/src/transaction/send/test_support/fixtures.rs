//! Exact transaction, driver, request, deadline, and failure fixtures.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use kafka_client_core::{
    Deadline, DeliveryStatus, OperationId, PartitionIndex, ProducerAttemptFailureKind, TopicId,
    TransactionEndOutcome, TransactionEpoch, TransactionLifecycleEffect, TransactionLifecycleInput,
    TransactionLifecycleMachine, TransactionalOwnerId,
};

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::{
        DriverOwner,
        transaction_produce::{TransactionProduceFailure, TransactionProduceFailureKind},
    },
    producer::{PublicProducerRecord, materialization::MaterializationRecord},
};

use super::super::{
    TransactionSendInput, TransactionSendRequest, port::TransactionSendProduceSubmissionFailure,
};

pub(in crate::transaction::send) fn request(
    epoch: TransactionEpoch,
    topic: &str,
    max_batch_bytes: usize,
) -> TransactionSendRequest {
    request_with_deadline(epoch, topic, max_batch_bytes, deadline(50))
}

pub(in crate::transaction::send) fn request_with_deadline(
    epoch: TransactionEpoch,
    topic: &str,
    max_batch_bytes: usize,
    deadline: OperationDeadline,
) -> TransactionSendRequest {
    let canonical_topic = Arc::<str>::from(topic);
    let input = TransactionSendInput::new(
        epoch,
        PublicProducerRecord::to(Arc::clone(&canonical_topic))
            .partition(2)
            .timestamp_milliseconds(1_000)
            .value(Bytes::from_static(b"value")),
        canonical_topic,
        Some(PartitionIndex::from_raw(2)),
        MaterializationRecord::new(1_000, None, Some(Bytes::from_static(b"value")), Vec::new()),
        topic.len() + b"value".len(),
        deadline,
    );
    TransactionSendRequest::try_prepare(input, TopicId::from_raw(9), max_batch_bytes)
        .unwrap_or_else(|error| panic!("test send request resolves: {error:?}"))
}

pub(in crate::transaction::send) fn automatic_request(
    epoch: TransactionEpoch,
    topic: &str,
    key: Option<Bytes>,
    max_batch_bytes: usize,
) -> TransactionSendRequest {
    let canonical_topic = Arc::<str>::from(topic);
    let input = TransactionSendInput::new(
        epoch,
        PublicProducerRecord::to(Arc::clone(&canonical_topic))
            .timestamp_milliseconds(1_000)
            .value(Bytes::from_static(b"value")),
        canonical_topic,
        None,
        MaterializationRecord::new(1_000, key, Some(Bytes::from_static(b"value")), Vec::new()),
        topic.len() + b"value".len(),
        deadline(50),
    );
    TransactionSendRequest::try_prepare(input, TopicId::from_raw(9), max_batch_bytes)
        .unwrap_or_else(|error| panic!("test automatic request prepares: {error:?}"))
}

pub(in crate::transaction::send) fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        Deadline::from_tick(tick),
        Instant::now() + Duration::from_secs(1),
    )
}

pub(in crate::transaction::send) fn driver() -> DriverOwner {
    DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error:?}"))
}

pub(in crate::transaction::send) fn local_submit_failure() -> TransactionSendProduceSubmissionFailure
{
    TransactionSendProduceSubmissionFailure {
        kind: ProducerAttemptFailureKind::LocalCapacity,
        delivery: DeliveryStatus::NotSent,
    }
}

pub(in crate::transaction::send) fn produce_failure(
    kind: TransactionProduceFailureKind,
    delivery: DeliveryStatus,
) -> TransactionProduceFailure {
    TransactionProduceFailure::for_test(kind, delivery)
}

pub(in crate::transaction::send) fn later_epoch() -> TransactionEpoch {
    let owner = TransactionalOwnerId::from_raw(88);
    let mut machine = TransactionLifecycleMachine::new(owner);
    let first = begin(&mut machine, owner);
    machine
        .apply(
            owner,
            TransactionLifecycleInput::Abort {
                epoch: first,
                operation_id: OperationId::from_raw(1),
            },
        )
        .unwrap_or_else(|error| panic!("abort: {error:?}"));
    machine
        .apply(
            owner,
            TransactionLifecycleInput::EndSettled {
                epoch: first,
                outcome: TransactionEndOutcome::Succeeded,
            },
        )
        .unwrap_or_else(|error| panic!("settle abort: {error:?}"));
    begin(&mut machine, owner)
}

fn begin(
    machine: &mut TransactionLifecycleMachine,
    owner: TransactionalOwnerId,
) -> TransactionEpoch {
    let transition = machine
        .apply(owner, TransactionLifecycleInput::Begin)
        .unwrap_or_else(|error| panic!("begin: {error:?}"));
    let Some(TransactionLifecycleEffect::Began { epoch, .. }) = transition.into_effect() else {
        panic!("begin effect");
    };
    epoch
}
