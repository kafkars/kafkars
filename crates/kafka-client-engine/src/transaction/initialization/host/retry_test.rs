//! Deterministic transaction-initialization retry schedule boundaries.

use kafka_client_core::{Deadline, Moment, ProducerRetryPolicy};

use super::retry::{TransactionInitializationRetrySchedule, plan_retry};

#[test]
fn schedule_retains_the_original_deadline_and_counts_one_replacement() {
    let policy = ProducerRetryPolicy::try_fixed(2, 5)
        .unwrap_or_else(|error| panic!("retry policy: {error}"));
    let deadline = Deadline::from_tick(200);

    assert_eq!(
        plan_retry(policy, 1, Moment::from_tick(100), deadline),
        Some(TransactionInitializationRetrySchedule {
            not_before: Deadline::from_tick(105),
            retries_started: 2,
        })
    );
    assert_eq!(deadline, Deadline::from_tick(200));
}

#[test]
fn zero_exhausted_and_deadline_blocked_schedules_are_rejected() {
    let deadline = Deadline::from_tick(100);
    assert_eq!(
        plan_retry(
            ProducerRetryPolicy::none(),
            0,
            Moment::from_tick(1),
            deadline,
        ),
        None
    );
    let one_retry = ProducerRetryPolicy::try_fixed(1, 1)
        .unwrap_or_else(|error| panic!("retry policy: {error}"));
    assert_eq!(
        plan_retry(one_retry, 1, Moment::from_tick(1), deadline),
        None
    );
    let five_tick_backoff = ProducerRetryPolicy::try_fixed(1, 5)
        .unwrap_or_else(|error| panic!("retry policy: {error}"));
    assert_eq!(
        plan_retry(five_tick_backoff, 0, Moment::from_tick(95), deadline,),
        None
    );
    assert_eq!(
        plan_retry(five_tick_backoff, 0, Moment::from_tick(100), deadline,),
        None
    );
}

#[test]
fn unrepresentable_backoff_does_not_create_a_retry() {
    let policy = ProducerRetryPolicy::try_fixed(1, u64::MAX)
        .unwrap_or_else(|error| panic!("retry policy: {error}"));
    assert_eq!(
        plan_retry(
            policy,
            0,
            Moment::from_tick(1),
            Deadline::from_tick(u64::MAX),
        ),
        None
    );
}
