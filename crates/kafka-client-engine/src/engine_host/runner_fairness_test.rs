//! Bounded-turn fairness under sustained producer pressure.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use kafka_client_core::PartitionIndex;

use crate::{
    EngineConfig, EngineProducerLimits,
    consumer::{GroupConsumerHandle, GroupConsumerRegistration},
    driver::TrackedProduceCalls,
    producer::ProducerRecord,
    transaction::TransactionInitializationRequest,
};

use super::{
    EngineHostResources, EngineLifecycle, finalize,
    runner::{HostTurnState, drive_host_turn},
    start,
    start_handoff::StartedEngineHost,
};

const PRODUCER_TURN_BUDGET: usize = 64;
const PRODUCER_BACKLOG: usize = PRODUCER_TURN_BUDGET * 2 + 1;
const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

#[test]
fn producer_saturation_does_not_starve_control_lanes() {
    let mut fixture = FairnessFixture::new();
    let producer_observers = fixture.prepare_producer_backlog(PRODUCER_BACKLOG);
    let admin_observer = fixture.admit_admin();
    let group = fixture.start_group_consumer();
    fixture.start_share_consumer();
    let transaction_observer = fixture.admit_transaction();
    let before = fixture.producer_stats();
    let driver_turns_before = fixture.started.control.snapshot().driver_turns;
    let mut state = HostTurnState::default();

    let first = drive_host_turn(fixture.resources(), &mut state)
        .unwrap_or_else(|error| panic!("first saturated host turn: {error}"));
    let after_first = fixture.producer_stats();

    assert_eq!(first.producer_admissions, PRODUCER_TURN_BUDGET);
    assert_eq!(
        before.host.prepared_batches - after_first.host.prepared_batches,
        PRODUCER_TURN_BUDGET,
    );
    assert_eq!(
        after_first.host.prepared_batches,
        PRODUCER_BACKLOG - PRODUCER_TURN_BUDGET,
    );
    assert!(first.producer_unsettled >= PRODUCER_BACKLOG);
    assert!(
        first.admin_progressed,
        "admin must advance in the same turn"
    );
    assert!(
        first.group_consumer_progressed,
        "group membership must advance in the same turn",
    );
    assert!(
        first.share_consumer_progressed,
        "share membership must advance in the same turn",
    );
    assert!(
        first.transaction_progressed,
        "transaction control must advance in the same turn",
    );
    assert!(first.driver_turned);
    assert!(!first.should_terminate);
    assert_eq!(
        fixture.started.control.snapshot().driver_turns,
        driver_turns_before + 1,
    );

    fixture.script_one_producer_completion();
    let second = drive_host_turn(fixture.resources(), &mut state)
        .unwrap_or_else(|error| panic!("second saturated host turn: {error}"));
    assert_eq!(second.producer_admissions, PRODUCER_TURN_BUDGET);
    assert!(second.driver_turned);
    assert!(
        second.producer_completions_progressed,
        "one scripted terminal must be applied in the next exact host turn",
    );

    drop((
        producer_observers,
        admin_observer,
        group,
        transaction_observer,
    ));
}

struct FairnessFixture {
    resources: Option<EngineHostResources>,
    started: StartedEngineHost,
}

impl FairnessFixture {
    fn new() -> Self {
        let limits = EngineProducerLimits::new(
            4 * 1024 * 1024,
            256,
            256,
            4 * 1024 * 1024,
            1,
            1024 * 1024,
            Duration::from_nanos(1),
        );
        let config = EngineConfig::new(vec!["127.0.0.1:1".to_owned()]).with_producer_limits(limits);
        let validated = config
            .validate()
            .unwrap_or_else(|error| panic!("validate fairness config: {error:?}"));
        let lifecycle = Arc::new(EngineLifecycle::new());
        let (mut resources, started) = start::prepare(&config, validated, &lifecycle)
            .unwrap_or_else(|error| panic!("prepare fairness host: {error}"));
        resources.produce_calls =
            TrackedProduceCalls::with_max_in_flight_requests_per_broker(256, 256);
        Self {
            resources: Some(resources),
            started,
        }
    }

    fn resources(&mut self) -> &mut EngineHostResources {
        self.resources
            .as_mut()
            .unwrap_or_else(|| panic!("fairness resources already finalized"))
    }

    fn prepare_producer_backlog(&mut self, count: usize) -> Vec<crate::ProducerDeliveryObserver> {
        let mut observers = Vec::with_capacity(count);
        for index in 0..count {
            let capture = self
                .started
                .clock
                .capture_deadline_after(OPERATION_TIMEOUT)
                .unwrap_or_else(|error| panic!("capture producer deadline {index}: {error}"));
            let accepted = self
                .started
                .admission
                .try_admit_explicit(
                    capture.now(),
                    capture.operation_deadline(),
                    ProducerRecord::new(
                        Arc::from(format!("fairness-{index}")),
                        PartitionIndex::from_raw(0),
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

    fn admit_admin(&self) -> crate::admin::DescribeClusterObserver {
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

    fn start_group_consumer(&self) -> GroupConsumerHandle {
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

    fn start_share_consumer(&self) -> kafka_client_core::GroupId {
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

    fn admit_transaction(&self) -> crate::transaction::TransactionInitializationObserver {
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

    fn producer_stats(&self) -> crate::producer::ingress::ProducerShardStats {
        self.started
            .admission
            .shard_stats()
            .unwrap_or_else(|error| panic!("observe producer stats: {error:?}"))
    }

    fn script_one_producer_completion(&mut self) {
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
