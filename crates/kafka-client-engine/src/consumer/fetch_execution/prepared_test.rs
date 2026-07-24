//! Directional core-ownership reconciliation for queued prepared Fetch work.

use kafka_client_core::{
    AssignedConsumerInput, AssignedConsumerMachineError, Deadline, FetchOwnership, FetchRecords,
    Moment,
};

use super::{
    DirectFetchExecutor, FetchAttemptDeadline, FetchExecutionError, FetchSubmission,
    PrepareFetchError, PreparedFetchExecution,
    admission_test::{
        assignment, fetch_fence, offset, operation_deadline, owner, prepared, shutdown,
    },
};

use crate::protocol::fetch::{FetchDecodeLimits, FetchRequestSettings};

#[test]
fn deadline_captured_for_another_fetch_revision_cannot_prepare_this_effect() {
    let (first, mut machine) = assignment(3, Deadline::from_tick(100));
    let first_fence = fetch_fence(first);
    let transition = machine
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: first_fence,
            records: FetchRecords::NoApplicationRecords,
            next_offset: offset(11),
            now: Moment::from_tick(1),
            throttle_ticks: 0,
        })
        .unwrap_or_else(|error| panic!("advance Fetch revision: {error}"));
    let [second] = transition.effects() else {
        panic!("one next FetchReady effect");
    };
    let second_fence = fetch_fence(*second);
    let attempt = FetchAttemptDeadline::from_parts_for_test(first_fence, operation_deadline(100));
    let error = PreparedFetchExecution::new(
        *second,
        "events".to_owned(),
        FetchRequestSettings::new(500, 1, 1_048_576, 1_048_576, 0),
        FetchDecodeLimits::default(),
        attempt,
        4_096,
    )
    .err()
    .unwrap_or_else(|| panic!("cross-wired attempt deadline must be rejected"));

    assert_eq!(
        error,
        PrepareFetchError::DeadlineFenceMismatch {
            effect: second_fence,
            captured: first_fence,
        }
    );
}

#[test]
fn future_fetch_fence_is_retained_as_a_fault_instead_of_discarded() {
    let (active, mut lagging) = assignment(3, Deadline::from_tick(100));
    let active_fence = fetch_fence(active);
    let (first, mut advanced) = assignment(3, Deadline::from_tick(100));
    let first_fence = fetch_fence(first);
    let transition = advanced
        .apply(AssignedConsumerInput::FetchAdvanced {
            fence: first_fence,
            records: FetchRecords::NoApplicationRecords,
            next_offset: offset(11),
            now: Moment::from_tick(1),
            throttle_ticks: 0,
        })
        .unwrap_or_else(|error| panic!("advance comparison machine: {error}"));
    let [future] = transition.effects() else {
        panic!("one future FetchReady effect");
    };
    let future_fence = fetch_fence(*future);
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, 4_096);
    let mut driver = owner();
    let (ownership_error, future_prepared) = prepared(*future, 100, 4_096)
        .reconcile_ownership(&lagging)
        .err()
        .unwrap_or_else(|| panic!("future fence must return its exact prepared owner"));

    assert_eq!(
        ownership_error,
        AssignedConsumerMachineError::StaleFetch {
            supplied: future_fence,
        }
    );
    assert_eq!(future_prepared.fence(), future_fence);
    assert_eq!(
        executor
            .submit(&driver, &mut lagging, future_prepared, Moment::from_tick(1),)
            .err(),
        Some(FetchExecutionError::Core(
            AssignedConsumerMachineError::StaleFetch {
                supplied: future_fence,
            }
        ))
    );
    assert_eq!(
        lagging.fetch_ownership(active_fence),
        Ok(FetchOwnership::Active)
    );
    assert_eq!(executor.retained(), (0, 0, 0));
    assert!(matches!(
        executor
            .submit(
                &driver,
                &mut lagging,
                prepared(active, 100, 4_096),
                Moment::from_tick(1),
            )
            .unwrap_or_else(|error| panic!("faulted executor returns ownership: {error:?}")),
        FetchSubmission::Unavailable(_)
    ));
    shutdown(&mut driver);
}
