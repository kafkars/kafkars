//! Stale live-terminal release after deterministic core fencing.

use kafka_client_core::{AssignedConsumerInput, Deadline, Moment, StartPosition};

use crate::protocol::fetch::fixture::encoded_data_batch_for_test;

use super::{
    DirectFetchExecutor,
    settlement_test::{
        OFFSET, OUTPUT_BYTES, TerminalFixture, assignment, fetch_fence, install, offset, prepared,
    },
};

#[test]
fn core_confirmed_stale_delivery_is_never_published_before_route_confirmation() {
    let (effect, mut machine) = assignment();
    let fence = fetch_fence(effect);
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, OUTPUT_BYTES);
    install(
        &mut executor,
        prepared(effect),
        TerminalFixture::Success(Some(encoded_data_batch_for_test(OFFSET))),
    );
    machine
        .apply(AssignedConsumerInput::Seek {
            assignment_epoch: fence.position().assignment_epoch(),
            partition: fence.position().partition(),
            position: StartPosition::Offset(offset(20)),
            now: Moment::from_tick(1),
            resolution_deadline: Deadline::from_tick(1_000_000_000),
        })
        .unwrap_or_else(|error| panic!("replace position: {error}"));

    assert!(
        executor
            .poll(&mut machine, Moment::from_tick(8))
            .unwrap_or_else(|error| panic!("discard stale Fetch: {error:?}"))
            .is_none()
    );
    assert!(
        executor
            .take_ready()
            .unwrap_or_else(|error| panic!("query stale delivery: {error:?}"))
            .is_none()
    );
    assert_eq!(executor.retained(), (0, 0, 0));
}
