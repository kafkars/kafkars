//! Fake aggregate boundary and focused send-test fixture facade.

use std::sync::{Arc, Mutex, atomic::AtomicBool, mpsc::sync_channel};

use kafka_client_core::{DeliveryStatus, TransactionEpoch, TransactionalOwnerId};

use crate::{
    producer::materialization::TransactionalMaterializationBatch,
    transaction::{
        TransactionExecutionLimits, TransactionLifecycleHost,
        completion::TransactionCompletionOwner,
        initialization::TransactionalOwnerParts,
        partition_enrollment::{
            TransactionPartitionEnrollmentFailureKind, TransactionPartitionEnrollmentFence,
            TransactionPartitionEnrollmentTerminal,
        },
    },
};

mod aggregate;
mod fixtures;
mod produce_port;

pub(super) use fixtures::{
    automatic_request, deadline, driver, later_epoch, local_submit_failure, produce_failure,
    request, request_with_deadline,
};
pub(super) use produce_port::FakeProducePort;

pub(super) struct FakeAggregate {
    pub(super) host: TransactionLifecycleHost,
    pub(super) epoch: TransactionEpoch,
    captured: Option<TransactionalMaterializationBatch>,
    terminal: Option<TransactionPartitionEnrollmentTerminal>,
    pub(super) local_enrollment: bool,
    pub(super) log: Arc<Mutex<Vec<&'static str>>>,
    completion: TransactionCompletionOwner,
}

impl FakeAggregate {
    pub(super) fn new() -> Self {
        Self::with_retry_policy(kafka_client_core::ProducerRetryPolicy::none())
    }

    pub(super) fn with_retry_policy(retry_policy: kafka_client_core::ProducerRetryPolicy) -> Self {
        let (sender, _receiver) = sync_channel(1);
        let completion = TransactionCompletionOwner::start()
            .unwrap_or_else(|error| panic!("completion owner starts: {error:?}"));
        let parts = TransactionalOwnerParts::new(
            TransactionalOwnerId::from_raw(7),
            Arc::<str>::from("writer"),
            41,
            3,
            Arc::new(AtomicBool::new(true)),
            sender,
            completion
                .lifecycle_publisher()
                .unwrap_or_else(|error| panic!("publisher: {error:?}")),
            completion
                .send_publisher()
                .unwrap_or_else(|error| panic!("send publisher: {error:?}")),
        );
        let mut host = TransactionLifecycleHost::try_new(
            parts,
            TransactionExecutionLimits::try_new_with_retry_policy(
                8,
                1024,
                kafka_client_core::CompressionPolicy::None,
                retry_policy,
            )
            .unwrap_or_else(|| panic!("limits")),
        )
        .unwrap_or_else(|(error, _)| panic!("lifecycle host: {error:?}"));
        let epoch = host
            .begin()
            .unwrap_or_else(|error| panic!("transaction begins: {error:?}"));
        Self {
            host,
            epoch,
            captured: None,
            terminal: None,
            local_enrollment: false,
            log: Arc::new(Mutex::new(Vec::new())),
            completion,
        }
    }

    pub(super) fn send_owner(
        &self,
        compression: kafka_client_core::CompressionPolicy,
    ) -> super::TransactionSendOwner {
        super::TransactionSendOwner::new(
            compression,
            8,
            self.completion
                .send_publisher()
                .unwrap_or_else(|error| panic!("send publisher: {error:?}")),
        )
    }

    pub(super) fn enrolled(&mut self) {
        let batch = self
            .captured
            .take()
            .unwrap_or_else(|| panic!("pending enrollment batch"));
        self.terminal = Some(TransactionPartitionEnrollmentTerminal::Enrolled(
            TransactionPartitionEnrollmentFence::new(self.epoch, batch),
        ));
    }

    pub(super) fn captured_partition(&self) -> Option<i32> {
        self.captured
            .as_ref()
            .map(TransactionalMaterializationBatch::partition)
    }

    pub(super) fn enrollment_abort_required(&mut self) {
        let batch = self
            .captured
            .take()
            .unwrap_or_else(|| panic!("pending enrollment batch"));
        self.terminal = Some(TransactionPartitionEnrollmentTerminal::AbortRequired {
            kind: TransactionPartitionEnrollmentFailureKind::Transport,
            delivery: DeliveryStatus::PossiblySent,
            batch,
        });
    }

    pub(super) fn enrollment_fatal(&mut self) {
        let batch = self
            .captured
            .take()
            .unwrap_or_else(|| panic!("pending enrollment batch"));
        self.terminal = Some(TransactionPartitionEnrollmentTerminal::Fatal {
            kind: TransactionPartitionEnrollmentFailureKind::Broker {
                code: 90,
                fenced: true,
            },
            batch,
        });
    }
}
