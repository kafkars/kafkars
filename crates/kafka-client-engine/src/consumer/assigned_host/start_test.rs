//! First-slice direct-consumer defaults remain bounded and internally valid.

use std::sync::Arc;

use kafka_client_core::ReadIsolation;

use super::{
    completion::AssignedConsumerCompletionNotifier, shard_test::CountingWake,
    start::build_first_assigned_consumer,
};
use crate::{clock::MonotonicClock, config::EngineConsumerFetchConfig};

#[test]
fn each_read_isolation_builds_one_matching_idle_core_owner() {
    for isolation in [ReadIsolation::ReadUncommitted, ReadIsolation::ReadCommitted] {
        let (mut notifier, publishers) = AssignedConsumerCompletionNotifier::start()
            .unwrap_or_else(|error| panic!("completion notifier: {error}"));
        let (owner, _port) = build_first_assigned_consumer(
            isolation,
            EngineConsumerFetchConfig::default()
                .validate()
                .unwrap_or_else(|error| panic!("Fetch config: {error:?}")),
            crate::config::ValidatedConsumerLimits::default(),
            Arc::new(MonotonicClock::new()),
            Arc::new(CountingWake::default()),
            publishers.close,
            publishers.recv,
            publishers.event,
        )
        .unwrap_or_else(|error| panic!("first assigned consumer: {error:?}"));

        assert_eq!(
            owner
                .try_with_owner(|assigned| {
                    (assigned.unsettled(), assigned.machine.read_isolation())
                })
                .unwrap_or_else(|error| panic!("owner slot: {error:?}")),
            (0, isolation)
        );
        drop(owner);
        let join = notifier
            .stop()
            .unwrap_or_else(|error| panic!("stop notifier: {error}"));
        join.join_off_notifier()
            .unwrap_or_else(|error| panic!("join notifier: {error}"));
    }
}
