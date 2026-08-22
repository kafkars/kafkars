//! Host-path evidence that initialization retries never strengthen delivery certainty.

use std::{sync::Arc, time::Duration};

use kafka_client_core::{Moment, ProducerRetryPolicy, TransactionInitializationInput};

use super::TransactionInitializationHost;
use crate::{
    EngineConfig,
    clock::MonotonicClock,
    driver::{DriverOwner, TransactionInitTerminal},
    transaction::initialization::{
        TransactionInitializationAccepted, TransactionInitializationAdmissionPort,
        TransactionInitializationDeliveryStatus, TransactionInitializationFailureKind,
        TransactionInitializationOutcome, TransactionInitializationRequest,
        TransactionInitializationShardOwner,
    },
};

#[test]
fn replacement_not_sent_rejection_cannot_strengthen_response_certainty() {
    for retry in ResponseRetry::ALL {
        let fixture = Fixture::new();
        let (accepted, _deadline) = fixture.schedule_response_retry(retry);
        fixture
            .shard
            .try_host()
            .unwrap_or_else(|error| panic!("host lock: {error:?}"))
            .apply(0, TransactionInitializationInput::DriverRejected)
            .unwrap_or_else(|error| panic!("replacement rejection: {error:?}"));

        assert_failure(
            accepted,
            TransactionInitializationFailureKind::DriverRejected,
        );
    }
}

#[test]
fn deadline_during_response_retry_backoff_preserves_uncertainty() {
    for retry in ResponseRetry::ALL {
        let fixture = Fixture::new();
        let (accepted, deadline) = fixture.schedule_response_retry(retry);
        fixture
            .shard
            .try_host()
            .unwrap_or_else(|error| panic!("host lock: {error:?}"))
            .turn(Moment::from_tick(deadline.tick()), &fixture.driver)
            .unwrap_or_else(|error| panic!("deadline turn: {error:?}"));

        assert_failure(
            accepted,
            TransactionInitializationFailureKind::DeadlineElapsed,
        );
    }
}

#[test]
fn shutdown_during_response_retry_backoff_preserves_uncertainty() {
    for retry in ResponseRetry::ALL {
        let fixture = Fixture::new();
        let (accepted, _deadline) = fixture.schedule_response_retry(retry);
        fixture
            .shard
            .try_host()
            .unwrap_or_else(|error| panic!("host lock: {error:?}"))
            .recover_after_driver_shutdown()
            .unwrap_or_else(|error| panic!("shutdown recovery: {error:?}"));

        assert_failure(
            accepted,
            TransactionInitializationFailureKind::DriverRejected,
        );
    }
}

#[test]
fn concurrent_transactions_retries_same_coordinator_once_then_exhausts() {
    let fixture = Fixture::new();
    let (accepted, deadline) =
        fixture.schedule_response_retry(ResponseRetry::ConcurrentTransactions);
    let mut host = fixture
        .shard
        .try_host()
        .unwrap_or_else(|error| panic!("host lock: {error:?}"));
    assert_eq!(host.operations[0].retries_started, 1);
    assert_eq!(
        host.operations[0].retry_not_before,
        Some(kafka_client_core::Deadline::from_tick(deadline.tick() - 1))
    );
    assert_eq!(host.operations[0].deadline.core(), deadline);
    host.apply(0, TransactionInitializationInput::DriverAccepted)
        .unwrap_or_else(|error| panic!("replacement acceptance: {error:?}"));
    host.settle_or_retry_terminal(
        0,
        TransactionInitTerminal::response_for_test(51),
        Moment::from_tick(deadline.tick() - 1),
    )
    .unwrap_or_else(|error| panic!("exhausted concurrent transaction retry: {error:?}"));
    drop(host);

    assert_failure(
        accepted,
        TransactionInitializationFailureKind::Broker {
            code: 51,
            fenced: false,
        },
    );
}

#[derive(Clone, Copy)]
enum ResponseRetry {
    RefreshedCoordinatorLoad,
    ConcurrentTransactions,
}

impl ResponseRetry {
    const ALL: [Self; 2] = [Self::RefreshedCoordinatorLoad, Self::ConcurrentTransactions];

    fn terminal(self) -> TransactionInitTerminal {
        match self {
            Self::RefreshedCoordinatorLoad => {
                TransactionInitTerminal::refreshed_response_for_test(14)
            }
            Self::ConcurrentTransactions => TransactionInitTerminal::response_for_test(51),
        }
    }
}

struct Fixture {
    driver: DriverOwner,
    shard: TransactionInitializationShardOwner,
    port: TransactionInitializationAdmissionPort,
}

impl Fixture {
    fn new() -> Self {
        let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
            .unwrap_or_else(|error| panic!("build embedded driver: {error}"));
        let retry_policy = ProducerRetryPolicy::try_fixed(1, 1)
            .unwrap_or_else(|error| panic!("retry policy: {error}"));
        let host = TransactionInitializationHost::start_with_retry_policy(retry_policy)
            .unwrap_or_else(|error| panic!("start transaction host: {error}"));
        let shard = TransactionInitializationShardOwner::new(
            host,
            Arc::new(MonotonicClock::new()),
            Arc::new(driver.reactor_wake()),
        );
        let port = shard.admission_port();
        Self {
            driver,
            shard,
            port,
        }
    }

    fn schedule_response_retry(
        &self,
        retry: ResponseRetry,
    ) -> (
        TransactionInitializationAccepted,
        kafka_client_core::Deadline,
    ) {
        let accepted = self
            .port
            .capture(Duration::from_secs(5), Arc::new(()))
            .unwrap_or_else(|error| panic!("capture deadline: {error:?}"))
            .initialize_transactional_owner(TransactionInitializationRequest::new(
                "delivery-floor-writer".to_owned(),
                45_000,
            ))
            .unwrap_or_else(|error| panic!("admit initialization: {:?}", error.kind()));
        let mut host = self
            .shard
            .try_host()
            .unwrap_or_else(|error| panic!("host lock: {error:?}"));
        let deadline = host
            .next_deadline()
            .unwrap_or_else(|| panic!("accepted operation retains its deadline"));
        assert!(deadline.tick() >= 2);
        host.apply(0, TransactionInitializationInput::DriverAccepted)
            .unwrap_or_else(|error| panic!("initial driver acceptance: {error:?}"));
        host.settle_or_retry_terminal(0, retry.terminal(), Moment::from_tick(deadline.tick() - 2))
            .unwrap_or_else(|error| panic!("schedule response retry: {error:?}"));
        assert_eq!(
            host.next_deadline(),
            Some(kafka_client_core::Deadline::from_tick(deadline.tick() - 1))
        );
        drop(host);
        (accepted, deadline)
    }
}

fn assert_failure(
    accepted: TransactionInitializationAccepted,
    expected_kind: TransactionInitializationFailureKind,
) {
    let TransactionInitializationOutcome::Failed(failure) = accepted
        .into_observer()
        .wait()
        .unwrap_or_else(|error| panic!("observe terminal: {error:?}"))
    else {
        panic!("replacement must fail initialization");
    };
    assert_eq!(failure.kind(), expected_kind);
    assert_eq!(
        failure.delivery(),
        TransactionInitializationDeliveryStatus::PossiblySent
    );
}
