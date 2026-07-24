//! First-slice direct-consumer defaults remain bounded and internally valid.

use std::sync::Arc;

use super::{shard_test::CountingWake, start::build_first_assigned_consumer};
use crate::clock::MonotonicClock;

#[test]
fn first_slice_builds_one_idle_bounded_owner() {
    let (owner, _port) = build_first_assigned_consumer(
        Arc::new(MonotonicClock::new()),
        Arc::new(CountingWake::default()),
    )
    .unwrap_or_else(|error| panic!("first assigned consumer: {error:?}"));

    assert_eq!(
        owner
            .try_with_owner(|assigned| assigned.unsettled())
            .unwrap_or_else(|error| panic!("owner slot: {error:?}")),
        0
    );
}
