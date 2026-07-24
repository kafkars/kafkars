//! Direct terminal-store, reclamation, drop-panic, and waker ticket scenarios.

use std::{
    sync::{Arc, mpsc::sync_channel},
    task::Poll,
};

use super::{
    CompletionObserver, PublishTicket,
    cell::CompletionCell,
    test_support::{CountingWake, PanicWake, poll_once},
};

#[test]
fn ticket_stores_terminal_wakes_observer_and_queues_reclaim() {
    let (sender, reclaim) = sync_channel(1);
    let cell = Arc::new(CompletionCell::new(0, sender));
    let id = cell
        .activate()
        .unwrap_or_else(|error| panic!("activate completion cell: {error}"));
    let mut observer = CompletionObserver::new(id, Arc::clone(&cell));
    let wake = CountingWake::new();
    assert_eq!(poll_once(&mut observer, Arc::clone(&wake)), Poll::Pending);

    PublishTicket::new(id, cell, 17).publish();

    assert_eq!(wake.count(), 1);
    assert_eq!(poll_once(&mut observer, wake), Poll::Ready(Ok(17)));
    assert_eq!(reclaim.try_recv(), Ok(id));
}

#[test]
fn abandoned_panicking_terminal_still_reclaims_and_cell_remains_usable() {
    let (sender, reclaim) = sync_channel(1);
    let cell = Arc::new(CompletionCell::new(0, sender));
    let first_id = cell
        .activate()
        .unwrap_or_else(|error| panic!("activate first completion: {error}"));
    let abandoned = CompletionObserver::new(first_id, Arc::clone(&cell));
    drop(abandoned);

    PublishTicket::new(first_id, Arc::clone(&cell), MaybePanicDrop(true)).publish();

    assert_eq!(reclaim.try_recv(), Ok(first_id));
    assert_eq!(cell.try_recycle(first_id), Ok(true));
    let second_id = cell
        .activate()
        .unwrap_or_else(|error| panic!("activate recycled completion: {error}"));
    let observer = CompletionObserver::new(second_id, Arc::clone(&cell));
    PublishTicket::new(second_id, cell, MaybePanicDrop(false)).publish();
    let terminal = observer
        .wait()
        .unwrap_or_else(|error| panic!("observe live terminal: {error}"));
    assert!(!terminal.0);
    assert_eq!(reclaim.try_recv(), Ok(second_id));
}

#[test]
fn panicking_waker_cannot_escape_ticket_publication() {
    let (sender, reclaim) = sync_channel(1);
    let cell = Arc::new(CompletionCell::new(0, sender));
    let id = cell
        .activate()
        .unwrap_or_else(|error| panic!("activate completion cell: {error}"));
    let mut observer = CompletionObserver::new(id, Arc::clone(&cell));
    assert_eq!(poll_once(&mut observer, Arc::new(PanicWake)), Poll::Pending);

    PublishTicket::new(id, cell, 23).publish();

    assert_eq!(observer.wait(), Ok(23));
    assert_eq!(reclaim.try_recv(), Ok(id));
}

#[derive(Debug, Eq, PartialEq)]
struct MaybePanicDrop(bool);

impl Drop for MaybePanicDrop {
    fn drop(&mut self) {
        assert!(!self.0, "intentional terminal drop panic");
    }
}
