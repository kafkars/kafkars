//! First-slice direct-consumer defaults remain bounded and internally valid.

use std::sync::Arc;

use super::{
    completion::AssignedConsumerCompletionNotifier, shard_test::CountingWake,
    start::build_first_assigned_consumer,
};
use crate::clock::MonotonicClock;

#[test]
fn first_slice_builds_one_idle_bounded_owner() {
    let (mut notifier, publisher) = AssignedConsumerCompletionNotifier::start()
        .unwrap_or_else(|error| panic!("completion notifier: {error}"));
    let (owner, _port) = build_first_assigned_consumer(
        Arc::new(MonotonicClock::new()),
        Arc::new(CountingWake::default()),
        publisher,
    )
    .unwrap_or_else(|error| panic!("first assigned consumer: {error:?}"));

    assert_eq!(
        owner
            .try_with_owner(|assigned| assigned.unsettled())
            .unwrap_or_else(|error| panic!("owner slot: {error:?}")),
        0
    );
    drop(owner);
    let join = notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"));
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("join notifier: {error}"));
}
