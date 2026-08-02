//! Production shard control from initialization through owner-loss cleanup.

use std::{sync::Arc, time::Duration};

use crate::{EngineConfig, clock::MonotonicClock, driver::DriverOwner};

use super::{
    TransactionInitializationAdmissionErrorKind, TransactionInitializationHost,
    TransactionInitializationOutcome, TransactionInitializationRequest,
    TransactionInitializationShardOwner, TransactionLifecycleControlError,
};

#[test]
fn initialized_owner_begins_and_admits_commit_with_an_original_deadline() {
    let fixture = Fixture::new();
    let owner = fixture.initialize(41);

    assert_eq!(owner.transactional_id(), "invoice-writer");
    assert_eq!((owner.producer_id(), owner.producer_epoch()), (41, 3));
    assert!(owner.is_active());
    let epoch = owner
        .begin()
        .unwrap_or_else(|error| panic!("initialized lifecycle begins: {error:?}"))
        .value;
    let accepted = owner
        .commit(epoch, Duration::from_secs(4))
        .unwrap_or_else(|error| {
            panic!("commit reserves its terminal before acceptance: {error:?}")
        });

    assert!(!accepted.wake_failed);
    assert!(
        fixture
            .shard
            .try_host()
            .unwrap_or_else(|error| panic!("host lock: {error:?}"))
            .next_deadline()
            .is_some()
    );
}

#[test]
fn control_reports_contended_closed_and_stale_without_hidden_queueing() {
    let fixture = Fixture::new();
    let owner = fixture.initialize(43);
    let owner_id = owner.owner_id_for_test();
    let control = owner.control_for_test();

    let lock = fixture
        .shard
        .try_host()
        .unwrap_or_else(|error| panic!("hold host lock: {error:?}"));
    assert!(matches!(
        owner.begin(),
        Err(TransactionLifecycleControlError::Contended)
    ));
    drop(lock);

    owner.close();
    {
        let mut host = fixture
            .shard
            .try_host()
            .unwrap_or_else(|error| panic!("cleanup host lock: {error:?}"));
        assert!(
            host.owner_loss_for_test()
                .unwrap_or_else(|error| panic!("idle owner loss: {error:?}"))
        );
        assert!(
            host.release_owner_for_test()
                .unwrap_or_else(|error| panic!("execution release: {error:?}"))
        );
        host.prune_closed_lifecycles_for_test();
    }
    assert!(matches!(
        control.begin(owner_id),
        Err(TransactionLifecycleControlError::StaleOwner)
    ));

    fixture.port.close_admission();
    assert!(matches!(
        control.begin(owner_id),
        Err(TransactionLifecycleControlError::Closed)
    ));
}

#[test]
fn closed_lifecycle_still_reserves_its_slot_until_pruned() {
    let fixture = Fixture::new();
    let mut owners = Vec::new();
    for producer_id in 1..=8 {
        owners.push(fixture.initialize(producer_id));
    }
    assert_eq!(
        fixture
            .shard
            .try_host()
            .unwrap_or_else(|error| panic!("host lock: {error:?}"))
            .lifecycle_count_for_test(),
        8
    );

    owners.pop().unwrap_or_else(|| panic!("last owner")).close();
    {
        let mut host = fixture
            .shard
            .try_host()
            .unwrap_or_else(|error| panic!("cleanup host lock: {error:?}"));
        assert!(
            host.owner_loss_for_test()
                .unwrap_or_else(|error| panic!("idle owner loss: {error:?}"))
        );
        assert!(
            host.release_owner_for_test()
                .unwrap_or_else(|error| panic!("execution release: {error:?}"))
        );
    }
    let Err(error) = fixture.try_initialize(90) else {
        panic!("retained closed lifecycle unexpectedly released bounded capacity");
    };
    assert_eq!(error, TransactionInitializationAdmissionErrorKind::Capacity);

    fixture
        .shard
        .try_host()
        .unwrap_or_else(|error| panic!("prune host lock: {error:?}"))
        .prune_closed_lifecycles_for_test();
    assert_eq!(fixture.initialize(90).producer_id(), 90);
}

#[test]
fn coordinator_refresh_progress_is_visible_and_shutdown_preserves_broker_terminal() {
    let fixture = Fixture::new();
    let accepted = fixture
        .port
        .capture(Duration::from_secs(5), Arc::new(()))
        .unwrap_or_else(|error| panic!("capture deadline: {error:?}"))
        .initialize_transactional_owner(TransactionInitializationRequest::new(
            "invoice-writer".to_owned(),
            45_000,
        ))
        .unwrap_or_else(|error| panic!("admit initialization: {:?}", error.kind()));
    {
        let mut host = fixture
            .shard
            .try_host()
            .unwrap_or_else(|error| panic!("host lock: {error:?}"));
        host.install_refresh_call_for_test(&fixture._driver, 14)
            .unwrap_or_else(|error| panic!("install refresh call: {error:?}"));
        assert_eq!(
            host.turn(kafka_client_core::Moment::from_tick(1), &fixture._driver)
                .unwrap_or_else(|error| panic!("refresh progress turn: {error:?}")),
            super::TransactionInitializationTurn::Progress
        );
        assert_eq!(
            host.turn(kafka_client_core::Moment::from_tick(2), &fixture._driver)
                .unwrap_or_else(|error| panic!("refresh pending turn: {error:?}")),
            super::TransactionInitializationTurn::Idle
        );
        host.recover_after_driver_shutdown()
            .unwrap_or_else(|error| panic!("refresh recovery: {error:?}"));
    }
    let TransactionInitializationOutcome::Failed(failure) = accepted
        .into_observer()
        .wait()
        .unwrap_or_else(|error| panic!("observe recovered terminal: {error:?}"))
    else {
        panic!("recovered coordinator rejection must fail initialization");
    };
    assert_eq!(
        failure.kind(),
        super::TransactionInitializationFailureKind::Broker {
            code: 14,
            fenced: false,
        }
    );
}

#[test]
fn stalled_refresh_expires_at_original_deadline_without_a_late_init_retry() {
    let fixture = Fixture::new();
    let accepted = fixture
        .port
        .capture(Duration::from_secs(5), Arc::new(()))
        .unwrap_or_else(|error| panic!("capture deadline: {error:?}"))
        .initialize_transactional_owner(TransactionInitializationRequest::new(
            "invoice-writer".to_owned(),
            45_000,
        ))
        .unwrap_or_else(|error| panic!("admit initialization: {:?}", error.kind()));
    {
        let mut host = fixture
            .shard
            .try_host()
            .unwrap_or_else(|error| panic!("host lock: {error:?}"));
        let deadline = host
            .next_deadline()
            .unwrap_or_else(|| panic!("accepted initialization owns its original deadline"));
        assert!(deadline.tick() >= 2);
        host.install_refresh_call_for_test(&fixture._driver, 14)
            .unwrap_or_else(|error| panic!("install refresh call: {error:?}"));
        assert_eq!(host.next_deadline(), Some(deadline));
        assert_eq!(
            host.turn(
                kafka_client_core::Moment::from_tick(deadline.tick() - 2),
                &fixture._driver,
            )
            .unwrap_or_else(|error| panic!("refresh progress turn: {error:?}")),
            super::TransactionInitializationTurn::Progress
        );
        assert_eq!(host.next_deadline(), Some(deadline));
        assert_eq!(
            host.turn(
                kafka_client_core::Moment::from_tick(deadline.tick() - 1),
                &fixture._driver,
            )
            .unwrap_or_else(|error| panic!("refresh pending turn: {error:?}")),
            super::TransactionInitializationTurn::Idle
        );
        assert_eq!(host.next_deadline(), Some(deadline));
        assert_eq!(
            host.turn(
                kafka_client_core::Moment::from_tick(deadline.tick()),
                &fixture._driver,
            )
            .unwrap_or_else(|error| panic!("refresh deadline turn: {error:?}")),
            super::TransactionInitializationTurn::Progress
        );
        assert_eq!(host.next_deadline(), None, "no replacement retry is armed");
    }

    let TransactionInitializationOutcome::Failed(failure) = accepted
        .into_observer()
        .wait()
        .unwrap_or_else(|error| panic!("observe deadline terminal: {error:?}"))
    else {
        panic!("expired refresh cannot initialize an owner");
    };
    assert_eq!(
        failure.kind(),
        super::TransactionInitializationFailureKind::DeadlineElapsed
    );
    assert_eq!(
        failure.delivery(),
        super::TransactionInitializationDeliveryStatus::PossiblySent
    );
}

pub(super) struct Fixture {
    _driver: DriverOwner,
    shard: TransactionInitializationShardOwner,
    port: super::TransactionInitializationAdmissionPort,
}

impl Fixture {
    pub(super) fn new() -> Self {
        let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
            .unwrap_or_else(|error| panic!("build embedded driver: {error}"));
        let host = TransactionInitializationHost::start()
            .unwrap_or_else(|error| panic!("start transaction host: {error}"));
        let shard = TransactionInitializationShardOwner::new(
            host,
            Arc::new(MonotonicClock::new()),
            Arc::new(driver.reactor_wake()),
        );
        let port = shard.admission_port();
        Self {
            _driver: driver,
            shard,
            port,
        }
    }

    pub(super) fn initialize(&self, producer_id: i64) -> super::TransactionalOwnerHandle {
        self.try_initialize(producer_id)
            .unwrap_or_else(|kind| panic!("initialize owner: {kind:?}"))
    }

    fn try_initialize(
        &self,
        producer_id: i64,
    ) -> Result<super::TransactionalOwnerHandle, TransactionInitializationAdmissionErrorKind> {
        let accepted = self
            .port
            .capture(Duration::from_secs(5), Arc::new(()))
            .unwrap_or_else(|error| panic!("capture deadline: {error:?}"))
            .initialize_transactional_owner(TransactionInitializationRequest::new(
                "invoice-writer".to_owned(),
                45_000,
            ))
            .map_err(|error| error.kind())?;
        self.shard
            .try_host()
            .unwrap_or_else(|error| panic!("host lock: {error:?}"))
            .initialize_for_test(producer_id, 3)
            .unwrap_or_else(|error| panic!("settle broker identity: {error:?}"));
        let outcome = accepted
            .into_observer()
            .wait()
            .unwrap_or_else(|error| panic!("observe initialization: {error:?}"));
        let wait_deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            if self
                .shard
                .try_host()
                .unwrap_or_else(|error| panic!("reclaim host lock: {error:?}"))
                .reclaim_for_test()
                .unwrap_or_else(|error| panic!("reclaim initialization: {error:?}"))
            {
                break;
            }
            assert!(
                std::time::Instant::now() < wait_deadline,
                "observed completion becomes reclaimable"
            );
            std::thread::yield_now();
        }
        match outcome {
            TransactionInitializationOutcome::Initialized(owner) => Ok(owner),
            TransactionInitializationOutcome::Failed(failure) => {
                panic!("initialization failed: {:?}", failure.kind())
            }
        }
    }
}
