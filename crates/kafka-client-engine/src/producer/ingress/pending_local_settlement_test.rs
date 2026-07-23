//! Bounded expiry and shutdown-drain progress scenarios.

use std::num::NonZeroUsize;

use kafka_client_core::{Deadline, Moment};

use crate::{
    clock::OperationDeadline,
    producer::{
        admission_test::record,
        host_limits_test::{start, valid_limits},
        pending::PendingSendRegistration,
    },
};

use super::{
    data::ProducerShardData,
    pending_local_settlement::{PendingLocalSettlementDisposition, PendingLocalSettlementProgress},
};

#[test]
fn expiry_is_bounded_and_reports_the_next_absolute_deadline() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let first = register(&mut data, "first", 5).into_send();
    let second = register(&mut data, "second", 5).into_send();
    let later = register(&mut data, "later", 10).into_send();

    let first_turn = settle(&mut data, 5, 1);
    assert_progress(
        first_turn,
        PendingLocalSettlementDisposition::Expiry,
        (1, 1, true, true, Some(Deadline::from_tick(5)), 1),
    );
    let second_turn = settle(&mut data, 5, 2);
    assert_progress(
        second_turn,
        PendingLocalSettlementDisposition::Expiry,
        (1, 1, true, false, Some(Deadline::from_tick(10)), 2),
    );
    drop((first, second, later));
}

#[test]
fn closed_shard_drains_a_bounded_fifo_prefix() {
    let mut data = ProducerShardData::new(start(valid_limits()));
    let first = register(&mut data, "first", 30).into_send();
    let second = register(&mut data, "second", 20).into_send();
    let third = register(&mut data, "third", 10).into_send();
    data.close_admission();

    let progress = settle(&mut data, 0, 2);

    assert_progress(
        progress,
        PendingLocalSettlementDisposition::ShutdownDrain,
        (2, 2, true, true, Some(Deadline::from_tick(10)), 2),
    );
    drop((first, second, third));
}

pub(super) fn settle(
    data: &mut ProducerShardData,
    now: u64,
    limit: usize,
) -> PendingLocalSettlementProgress {
    data.settle_pending_local(Moment::from_tick(now), nonzero(limit))
        .unwrap_or_else(|_refused| panic!("first local-settlement fault should install"))
}

pub(super) fn register(
    data: &mut ProducerShardData,
    topic: &str,
    tick: u64,
) -> PendingSendRegistration {
    data.register_pending(record(topic), deadline(tick))
        .unwrap_or_else(|error| panic!("pending fixture should register: {error:?}"))
}

pub(super) fn assert_progress(
    progress: PendingLocalSettlementProgress,
    disposition: PendingLocalSettlementDisposition,
    expected: (usize, usize, bool, bool, Option<Deadline>, usize),
) {
    assert_eq!(progress.disposition(), disposition);
    assert_eq!(progress.inspected(), expected.0);
    assert_eq!(progress.notifications_retained(), expected.1);
    assert_eq!(progress.pending_owned(), expected.2);
    assert_eq!(progress.runnable(), expected.3);
    assert_eq!(progress.next_deadline(), expected.4);
    assert_eq!(progress.route_pending(), expected.5);
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("test turn limit must be nonzero"))
}

fn deadline(tick: u64) -> OperationDeadline {
    OperationDeadline::from_parts_for_test(Deadline::from_tick(tick), std::time::Instant::now())
}
