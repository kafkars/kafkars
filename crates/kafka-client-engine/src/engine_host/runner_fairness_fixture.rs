//! Loopback-backed resources for bounded host-turn fairness evidence.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use kafka_client_core::PartitionIndex;

use crate::{
    EngineConfig, EngineProducerLimits,
    consumer::{GroupConsumerHandle, GroupConsumerRegistration},
    driver::{RoutedBroker, TrackedProduceCalls},
    producer::ProducerRecord,
    transaction::TransactionInitializationRequest,
};

use super::super::{
    EngineHostResources, EngineLifecycle, finalize, start, start_handoff::StartedEngineHost,
};

const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) struct FairnessFixture {
    pub(super) resources: Option<EngineHostResources>,
    started: StartedEngineHost,
    pub(super) broker: RoutedBroker,
}

impl FairnessFixture {
    pub(super) fn new(partition_count: usize) -> Self {
        let mut broker = RoutedBroker::with_available_topic("fairness", partition_count);
        let limits = EngineProducerLimits::new(
            4 * 1024 * 1024,
            256,
            256,
            4 * 1024 * 1024,
            1,
            1024 * 1024,
            Duration::from_nanos(1),
        );
        let config = EngineConfig::new(vec![broker.endpoint()]).with_producer_limits(limits);
        let validated = config
            .validate()
            .unwrap_or_else(|error| panic!("validate fairness config: {error:?}"));
        let lifecycle = Arc::new(EngineLifecycle::new());
        let (mut resources, started) = start::prepare(&config, validated, &lifecycle)
            .unwrap_or_else(|error| panic!("prepare fairness host: {error}"));
        let driver = resources
            .driver
            .as_mut()
            .unwrap_or_else(|| panic!("prepared fairness driver"));
        RoutedBroker::await_seed(driver);
        broker.install_cluster(driver);
        resources.produce_calls =
            TrackedProduceCalls::with_max_in_flight_requests_per_broker(256, 256);
        Self {
            resources: Some(resources),
            started,
            broker,
        }
    }

    pub(super) fn resources(&mut self) -> &mut EngineHostResources {
        self.resources
            .as_mut()
            .unwrap_or_else(|| panic!("fairness resources already finalized"))
    }

    pub(super) fn prepare_producer_backlog(
        &mut self,
        count: usize,
    ) -> Vec<crate::ProducerDeliveryObserver> {
        let mut observers = Vec::with_capacity(count);
        let capture = self
            .started
            .clock
            .capture_deadline_after(OPERATION_TIMEOUT)
            .unwrap_or_else(|error| panic!("capture shared producer deadline: {error}"));
        let topic: Arc<str> = Arc::from("fairness");
        for index in 0..count {
            let partition = u32::try_from(index)
                .unwrap_or_else(|error| panic!("producer partition {index}: {error}"));
            let accepted = self
                .started
                .admission
                .try_admit_explicit(
                    capture.now(),
                    capture.operation_deadline(),
                    ProducerRecord::new(
                        Arc::clone(&topic),
                        PartitionIndex::from_raw(partition),
                        1,
                        None,
                        Some(Bytes::from_static(b"x")),
                    ),
                )
                .unwrap_or_else(|error| panic!("admit producer record {index}: {error:?}"));
            let (observer, operation_id, fault) = accepted.into_parts();
            assert!(operation_id.is_some());
            assert!(fault.is_ok());
            observers.push(observer);
        }

        let clock = Arc::clone(&self.started.clock);
        let resources = self.resources();
        let mut data = resources
            .producer
            .try_data()
            .unwrap_or_else(|error| panic!("lock producer backlog: {error:?}"));
        for _turn in 0..16 {
            let now = clock
                .now()
                .unwrap_or_else(|error| panic!("observe producer setup clock: {error}"));
            data.turn(now, resources.budget)
                .unwrap_or_else(|error| panic!("prepare producer backlog: {error}"));
            crate::producer::test_identity::acquire_shard_if_pending(&mut data, now);
            let stats = data.shard_stats().host;
            if stats.prepared_batches == count && stats.submission_deadlines == count {
                return observers;
            }
        }
        let stats = data.shard_stats().host;
        panic!(
            "producer backlog did not become driver-ready: batches={}, deadlines={}",
            stats.prepared_batches, stats.submission_deadlines,
        );
    }

    pub(super) fn admit_admin(&self) -> crate::admin::DescribeClusterObserver {
        let capture = self
            .started
            .clock
            .capture_deadline_after(OPERATION_TIMEOUT)
            .unwrap_or_else(|error| panic!("capture admin deadline: {error}"));
        self.started
            .describe_cluster_admission
            .try_admit(capture.now(), capture.operation_deadline())
            .unwrap_or_else(|error| panic!("admit DescribeCluster: {error:?}"))
            .observer
    }

    pub(super) fn start_group_consumer(&self) -> GroupConsumerHandle {
        let lifetime: Arc<dyn Send + Sync> = Arc::new(());
        let mut group = GroupConsumerHandle::try_register(
            self.started.group_consumer.clone(),
            lifetime,
            GroupConsumerRegistration::new(Arc::from("fairness-group"), vec![Arc::from("orders")]),
        )
        .unwrap_or_else(|error| panic!("register group consumer: {error}"));
        let accepted = group
            .try_start(OPERATION_TIMEOUT)
            .unwrap_or_else(|error| panic!("start group consumer: {error}"));
        assert!(!accepted.entry_faulted());
        assert!(!accepted.wake_failed());
        group
    }

    pub(super) fn start_share_consumer(&self) -> kafka_client_core::GroupId {
        let registration = self
            .started
            .share_consumer
            .try_register(
                Arc::from("fairness-share"),
                None,
                vec![Arc::from("jobs")],
                crate::EngineShareConsumerFetchConfig::default(),
            )
            .unwrap_or_else(|failure| panic!("register share consumer: {:?}", failure.source));
        let capture = self
            .started
            .share_consumer
            .capture_deadline_after(OPERATION_TIMEOUT)
            .unwrap_or_else(|error| panic!("capture share deadline: {error}"));
        let accepted = self
            .started
            .share_consumer
            .try_begin(registration.group_id(), capture)
            .unwrap_or_else(|error| panic!("start share consumer: {error:?}"));
        assert!(!accepted.wake_failed());
        registration.group_id()
    }

    pub(super) fn admit_transaction(
        &self,
    ) -> crate::transaction::TransactionInitializationObserver {
        self.started
            .transaction_initialization
            .capture(OPERATION_TIMEOUT, Arc::new(()))
            .unwrap_or_else(|error| panic!("capture transaction deadline: {error:?}"))
            .initialize_transactional_owner(TransactionInitializationRequest::new(
                "fairness-transaction".to_owned(),
                30_000,
            ))
            .unwrap_or_else(|error| panic!("admit transaction: {:?}", error.kind()))
            .into_observer()
    }

    pub(super) fn producer_stats(&self) -> crate::producer::ingress::ProducerShardStats {
        self.started
            .admission
            .shard_stats()
            .unwrap_or_else(|error| panic!("observe producer stats: {error:?}"))
    }

    pub(super) fn driver_turns(&self) -> u64 {
        self.started.control.snapshot().driver_turns
    }

    pub(super) fn script_one_producer_completion(&mut self) {
        let now = self
            .started
            .clock
            .now()
            .unwrap_or_else(|error| panic!("observe scripted completion time: {error}"));
        self.resources()
            .produce_calls
            .settle_first_as_transport_failure_for_test(now);
    }
}

impl Drop for FairnessFixture {
    fn drop(&mut self) {
        let Some(resources) = self.resources.take() else {
            return;
        };
        self.started.control.request_failure();
        finalize::finish_host(resources, &self.started.lifecycle);
    }
}
