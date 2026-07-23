//! Permit reuse and hostile-notifier bound scenarios.

use std::{sync::Arc, task::Poll, time::Instant};

use bytes::Bytes;
use kafka_client_core::{
    Deadline, DeliveryStatus, PartitionIndex, ProducerCompletion, ProducerFailure,
};

use super::{
    PendingAdmissionRegistry, PendingAdmissionRejectionReason, ProducerSendFailure,
    ProducerSendFailureKind,
    test_support::{GateWake, poll_completion},
};
use crate::{
    clock::OperationDeadline,
    completion::{CompletionRegistry, NotificationBudget},
    producer::ProducerRecord,
};

#[test]
fn notification_permit_is_not_reused_before_dispatch() {
    let mut pending = PendingAdmissionRegistry::new(2, 128, 1);
    let first = register(&mut pending, record("first"));
    let first_send = first.into_send();
    let attempt = take(&mut pending);
    let local = attempt
        .settle_local(ProducerSendFailure::new(
            ProducerSendFailureKind::Backpressure,
        ))
        .unwrap_or_else(|_failure| panic!("first attempt should settle"));
    let (_admission, job) = local.into_parts();
    let Err(rejected) = pending.register(
        record("blocked"),
        OperationDeadline::from_parts_for_test(Deadline::from_tick(50), Instant::now()),
    ) else {
        panic!("live notification job must retain its permit");
    };
    assert_eq!(
        rejected.reason(),
        PendingAdmissionRejectionReason::NotificationBackpressure
    );
    let returned = rejected.into_record();
    assert_eq!(returned.topic().as_ref(), "blocked");

    job.dispatch_pending_notification_for_test();
    assert!(first_send.wait().is_err());
    let registration = pending
        .register(
            returned,
            OperationDeadline::from_parts_for_test(Deadline::from_tick(50), Instant::now()),
        )
        .unwrap_or_else(|error| panic!("dispatch should release permit: {error:?}"));
    let id = registration.id();
    drop(registration.into_send());
    drop(
        pending
            .cancel(id)
            .unwrap_or_else(|error| panic!("dropped registration should cancel: {error:?}")),
    );
}

#[test]
fn blocked_notifier_cannot_exceed_pending_notification_bound() {
    let budget = NotificationBudget::try_new(1, 2, 3)
        .unwrap_or_else(|error| panic!("notification budget should validate: {error:?}"));
    let owners = budget
        .start()
        .unwrap_or_else(|error| panic!("completion notifier should start: {error}"));
    let (mut completions, permits) = owners.into_parts();
    let (completion_id, mut completion) = completions
        .reserve()
        .unwrap_or_else(|error| panic!("completion should reserve: {error}"));
    let gate = GateWake::new();
    assert_eq!(
        poll_completion(&mut completion, gate.clone()),
        Poll::Pending
    );
    assert_eq!(
        completions.publish(
            completion_id,
            ProducerCompletion::Failed(ProducerFailure::execution_unavailable(
                DeliveryStatus::NotSent,
            )),
        ),
        Ok(())
    );
    assert!(gate.wait_until_entered());

    let mut pending = PendingAdmissionRegistry::with_notification_permits(3, 256, permits);
    let first = register(&mut pending, record("first")).into_send();
    let second = register(&mut pending, record("second")).into_send();
    let first_job = settle_next(&mut pending);
    let second_job = settle_next(&mut pending);
    notify(&completions, first_job);
    notify(&completions, second_job);
    let Err(rejected) = pending.register(
        record("bounded"),
        OperationDeadline::from_parts_for_test(Deadline::from_tick(50), Instant::now()),
    ) else {
        panic!("two blocked jobs must consume the complete pending bound");
    };
    assert_eq!(
        rejected.reason(),
        PendingAdmissionRejectionReason::NotificationBackpressure
    );
    let returned = rejected.into_record();

    gate.release();
    assert!(first.wait().is_err());
    assert!(second.wait().is_err());
    for _attempt in 0..10_000 {
        if pending.stats().notification_permits == 0 {
            break;
        }
        std::thread::yield_now();
    }
    assert_eq!(pending.stats().notification_permits, 0);
    let third = pending
        .register(
            returned,
            OperationDeadline::from_parts_for_test(Deadline::from_tick(50), Instant::now()),
        )
        .unwrap_or_else(|error| panic!("dispatched jobs should release permits: {error:?}"));
    let third_id = third.id();
    drop(third.into_send());
    drop(
        pending
            .cancel(third_id)
            .unwrap_or_else(|error| panic!("third registration should cancel: {error:?}")),
    );
    assert!(completion.wait().is_ok());
    reclaim_and_stop(&mut completions);
}

#[test]
fn pending_first_jobs_leave_every_terminal_lane_available() {
    let budget = NotificationBudget::try_new(2, 2, 4)
        .unwrap_or_else(|error| panic!("notification budget should validate: {error:?}"));
    let owners = budget
        .start()
        .unwrap_or_else(|error| panic!("completion notifier should start: {error}"));
    let (mut completions, permits) = owners.into_parts();
    let mut pending = PendingAdmissionRegistry::with_notification_permits(2, 256, permits);

    let (blocker_id, mut blocker) = completions
        .reserve()
        .unwrap_or_else(|error| panic!("blocker completion should reserve: {error}"));
    let gate = GateWake::new();
    assert_eq!(poll_completion(&mut blocker, gate.clone()), Poll::Pending);
    assert_eq!(completions.publish(blocker_id, terminal_failure()), Ok(()));
    assert!(gate.wait_until_entered());

    let first = register(&mut pending, record("first")).into_send();
    let second = register(&mut pending, record("second")).into_send();
    notify(&completions, settle_next(&mut pending));
    notify(&completions, settle_next(&mut pending));
    let (terminal_id, terminal) = completions
        .reserve()
        .unwrap_or_else(|error| panic!("last terminal lane should reserve: {error}"));
    assert_eq!(
        completions.publish(terminal_id, terminal_failure()),
        Ok(()),
        "P pending jobs must not consume the Nth terminal queue lane"
    );

    gate.release();
    assert!(first.wait().is_err());
    assert!(second.wait().is_err());
    assert!(blocker.wait().is_ok());
    assert!(terminal.wait().is_ok());
    reclaim_many_and_stop(&mut completions, 2);
}

fn settle_next(pending: &mut PendingAdmissionRegistry) -> super::PendingNotificationJob {
    let attempt = take(pending);
    let local = attempt
        .settle_local(ProducerSendFailure::new(
            ProducerSendFailureKind::Backpressure,
        ))
        .unwrap_or_else(|_failure| panic!("pending attempt should settle"));
    local.into_parts().1
}

fn take(pending: &mut PendingAdmissionRegistry) -> super::PendingPromotionAttempt {
    pending
        .take_next(1)
        .unwrap_or_else(|error| panic!("promotion take should succeed: {error:?}"))
        .into_attempt()
        .unwrap_or_else(|| panic!("promotion attempt should exist"))
}

fn register(
    pending: &mut PendingAdmissionRegistry,
    record: ProducerRecord,
) -> super::PendingSendRegistration {
    pending
        .register(
            record,
            OperationDeadline::from_parts_for_test(Deadline::from_tick(50), Instant::now()),
        )
        .unwrap_or_else(|error| panic!("pending registration should succeed: {error:?}"))
}

fn notify(
    completions: &CompletionRegistry<ProducerCompletion>,
    job: super::PendingNotificationJob,
) {
    if let Err((error, _job)) = completions.notify_pending(job) {
        panic!("pending notification should queue: {error}");
    }
}

fn reclaim_and_stop(completions: &mut CompletionRegistry<ProducerCompletion>) {
    reclaim_many_and_stop(completions, 1);
}

fn reclaim_many_and_stop(
    completions: &mut CompletionRegistry<ProducerCompletion>,
    expected: usize,
) {
    let mut reclaimed = 0;
    for _attempt in 0..10_000 {
        match completions.next_reclaim() {
            Ok(Some(id)) => {
                assert_eq!(
                    completions.finish_reclaim(id),
                    Ok(crate::completion::ReclaimStatus::Reclaimed)
                );
                reclaimed += 1;
                if reclaimed == expected {
                    let join = completions
                        .stop_notifier()
                        .unwrap_or_else(|error| panic!("completion notifier should stop: {error}"));
                    assert_eq!(join.join_off_notifier(), Ok(()));
                    return;
                }
            }
            Ok(None) => std::thread::yield_now(),
            Err(error) => panic!("completion reclaim should remain connected: {error}"),
        }
    }
    panic!("completion should become reclaimable");
}

fn terminal_failure() -> ProducerCompletion {
    ProducerCompletion::Failed(ProducerFailure::execution_unavailable(
        DeliveryStatus::NotSent,
    ))
}

fn record(topic: &str) -> ProducerRecord {
    ProducerRecord::new(
        Arc::from(topic),
        PartitionIndex::from_raw(0),
        1,
        None,
        Some(Bytes::from_static(b"value")),
    )
}
