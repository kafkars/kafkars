//! Exact execution owner, send request, driver, and Produce evidence fixtures.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::sync_channel,
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use kafka_client_core::{
    CompressionPolicy, Deadline, Moment, PartitionIndex, TransactionEpoch, TransactionSendId,
    TransactionalOwnerId,
};

use crate::{
    EngineConfig,
    clock::OperationDeadline,
    driver::{
        DriverOwner,
        transaction_produce::{TransactionProduceRouteRefreshPoll, TransactionProduceTerminalFact},
    },
    producer::{PublicProducerRecord, materialization::MaterializationRecord},
    transaction::{
        TransactionExecutionHost, TransactionExecutionLimits,
        completion::TransactionCompletionOwner,
        initialization::TransactionalOwnerParts,
        send::{
            TransactionSendInput, TransactionSendProduceCall, TransactionSendProduceEvidence,
            TransactionSendProducePort, TransactionSendProduceRequest,
            TransactionSendProduceSubmissionFailure,
        },
    },
};

pub(super) struct Fixture {
    pub(super) host: TransactionExecutionHost,
    pub(super) owner_id: TransactionalOwnerId,
    pub(super) driver: DriverOwner,
    _completion: TransactionCompletionOwner,
}

impl Fixture {
    pub(super) fn new(compression: CompressionPolicy) -> Self {
        Self::with_limits(compression, 8, 1_024, 1_024, 1_024)
    }

    pub(super) fn with_limits(
        compression: CompressionPolicy,
        partition_capacity: usize,
        retained_topic_bytes: usize,
        retained_record_bytes: usize,
        max_wire_batch_bytes: usize,
    ) -> Self {
        let owner_id = TransactionalOwnerId::from_raw(17);
        let (release, _released) = sync_channel(1);
        let completion = TransactionCompletionOwner::start()
            .unwrap_or_else(|error| panic!("completion owner: {error:?}"));
        let parts = TransactionalOwnerParts::new(
            owner_id,
            Arc::<str>::from("writer"),
            41,
            3,
            Arc::new(AtomicBool::new(true)),
            release,
            completion
                .lifecycle_publisher()
                .unwrap_or_else(|error| panic!("lifecycle publisher: {error:?}")),
            completion
                .send_publisher()
                .unwrap_or_else(|error| panic!("send publisher: {error:?}")),
            completion
                .offset_commit_publisher()
                .unwrap_or_else(|error| panic!("offset publisher: {error:?}")),
        );
        let limits = TransactionExecutionLimits::try_new_with_producer_bounds(
            partition_capacity,
            retained_topic_bytes,
            retained_record_bytes,
            max_wire_batch_bytes,
            compression,
        )
        .unwrap_or_else(|| panic!("limits"));
        let host = TransactionExecutionHost::try_new(parts, limits)
            .unwrap_or_else(|(error, _parts)| panic!("execution host: {error:?}"));
        let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
            .unwrap_or_else(|error| panic!("driver: {error:?}"));
        Self {
            host,
            owner_id,
            driver,
            _completion: completion,
        }
    }

    pub(super) fn shutdown_driver(&mut self) {
        self.driver
            .shutdown_with_turn_limit(64, Duration::from_millis(10))
            .unwrap_or_else(|error| panic!("driver shuts down: {error:?}"));
    }
}

pub(super) fn request(
    epoch: TransactionEpoch,
    topic: &str,
    deadline: OperationDeadline,
    retained_source_bytes: usize,
) -> TransactionSendInput {
    let canonical_topic = Arc::<str>::from(topic);
    TransactionSendInput::try_new(
        epoch,
        PublicProducerRecord::to(Arc::clone(&canonical_topic))
            .partition(2)
            .timestamp_milliseconds(1_000)
            .value(Bytes::from_static(b"value")),
        canonical_topic,
        Some(PartitionIndex::from_raw(2)),
        MaterializationRecord::new(1_000, None, Some(Bytes::from_static(b"value")), Vec::new()),
        retained_source_bytes,
        deadline,
    )
    .unwrap_or_else(|record| panic!("test send input allocates: {record:?}"))
}

pub(super) fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(
        Deadline::from_tick(tick),
        Instant::now() + Duration::from_secs(1),
    )
}

pub(super) struct FakeProducePort {
    fact: Option<TransactionProduceTerminalFact>,
    pub(super) observed_deadline: Option<OperationDeadline>,
    pub(super) discarded: Arc<AtomicBool>,
}

impl FakeProducePort {
    pub(super) fn succeeding(epoch: TransactionEpoch, send_id: TransactionSendId) -> Self {
        Self {
            fact: Some(TransactionProduceTerminalFact::Succeeded {
                epoch,
                send_id,
                success: kafka_client_core::ProducerBatchSuccess::new(42, None, None),
            }),
            observed_deadline: None,
            discarded: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(super) fn was_discarded(&self) -> bool {
        self.discarded.load(Ordering::Acquire)
    }
}

impl TransactionSendProducePort for FakeProducePort {
    fn submit(
        &mut self,
        request: TransactionSendProduceRequest<'_>,
    ) -> Result<Box<dyn TransactionSendProduceCall>, TransactionSendProduceSubmissionFailure> {
        self.observed_deadline = Some(request.deadline);
        Ok(Box::new(FakeProduceCall {
            attempt: request.attempt,
            fact: self.fact.take(),
            discarded: Arc::clone(&self.discarded),
        }))
    }
}

struct FakeProduceCall {
    attempt: kafka_client_core::TransactionSendAttempt,
    fact: Option<TransactionProduceTerminalFact>,
    discarded: Arc<AtomicBool>,
}

impl TransactionSendProduceCall for FakeProduceCall {
    fn try_terminal(&mut self) -> Option<Box<dyn TransactionSendProduceEvidence>> {
        self.fact.take().map(|fact| {
            Box::new(FakeProduceEvidence {
                attempt: self.attempt,
                fact,
                discarded: Arc::clone(&self.discarded),
            }) as Box<_>
        })
    }

    fn recover_after_driver_shutdown(self: Box<Self>) -> Box<dyn TransactionSendProduceEvidence> {
        Box::new(FakeProduceEvidence {
            attempt: self.attempt,
            fact: self.fact.unwrap_or_else(|| panic!("recovery fact")),
            discarded: Arc::clone(&self.discarded),
        })
    }
}

struct FakeProduceEvidence {
    attempt: kafka_client_core::TransactionSendAttempt,
    fact: TransactionProduceTerminalFact,
    discarded: Arc<AtomicBool>,
}

impl TransactionSendProduceEvidence for FakeProduceEvidence {
    fn attempt(&self) -> kafka_client_core::TransactionSendAttempt {
        self.attempt
    }

    fn fact(&self) -> TransactionProduceTerminalFact {
        self.fact
    }

    fn poll_route_refresh(&mut self, _driver: &DriverOwner) -> TransactionProduceRouteRefreshPoll {
        TransactionProduceRouteRefreshPoll::Failed
    }

    fn discard(self: Box<Self>) {
        self.discarded.store(true, Ordering::Release);
    }
}

pub(super) fn drive_send(fixture: &mut Fixture, port: &mut FakeProducePort, turns: u64) {
    for tick in 1..=turns {
        fixture
            .host
            .turn_with_produce_port_for_test(Moment::from_tick(tick), &fixture.driver, port)
            .unwrap_or_else(|error| panic!("execution turn: {error:?}"));
    }
}
