//! Direct Fetch-session establishment, reuse, and epoch progression scenarios.

use kafka_client_core::{AssignedConsumerEffect, Moment};

use super::{
    DirectFetchExecutor, PreparedFetchExecution,
    settlement_test::{OUTPUT_BYTES, assignment, prepared},
};

#[test]
fn direct_fetch_establishes_and_reuses_one_exact_partition_session() {
    let (first, mut machine) = assignment();
    let mut executor = DirectFetchExecutor::create_unbound(1, 1, OUTPUT_BYTES);
    executor
        .try_enable_sessions(1)
        .unwrap_or_else(|()| panic!("reserve direct Fetch-session state"));
    install_session(&mut executor, prepared(first), 91, true);

    let transition = executor
        .poll(&mut machine, Moment::from_tick(8))
        .unwrap_or_else(|error| panic!("settle initial session Fetch: {error:?}"))
        .unwrap_or_else(|| panic!("initial session transition"));
    let second = next_fetch(transition.effects());
    let (mut second_request, second_output) = prepared(second).into_parts_for_test();
    executor.bind_fetch_session(&mut second_request);
    assert_eq!(
        (
            second_request.session().session_id(),
            second_request.session().session_epoch()
        ),
        (91, 1)
    );
    install_session_parts(&mut executor, second_request, second_output, 91, false);

    let transition = executor
        .poll(&mut machine, Moment::from_tick(9))
        .unwrap_or_else(|error| panic!("settle incremental session Fetch: {error:?}"))
        .unwrap_or_else(|| panic!("incremental session transition"));
    let (mut third_request, _output) =
        prepared(next_fetch(transition.effects())).into_parts_for_test();
    executor.bind_fetch_session(&mut third_request);
    assert_eq!(
        (
            third_request.session().session_id(),
            third_request.session().session_epoch()
        ),
        (91, 2)
    );
}

fn next_fetch(effects: &[AssignedConsumerEffect]) -> AssignedConsumerEffect {
    effects
        .iter()
        .copied()
        .find(|effect| matches!(effect, AssignedConsumerEffect::FetchReady { .. }))
        .unwrap_or_else(|| panic!("next session Fetch"))
}

fn install_session(
    executor: &mut DirectFetchExecutor,
    prepared: PreparedFetchExecution,
    response_session_id: i32,
    include_partition: bool,
) {
    let (mut request, output_bytes) = prepared.into_parts_for_test();
    executor.bind_fetch_session(&mut request);
    assert_eq!(request.session().session_epoch(), 0);
    install_session_parts(
        executor,
        request,
        output_bytes,
        response_session_id,
        include_partition,
    );
}

fn install_session_parts(
    executor: &mut DirectFetchExecutor,
    request: crate::driver::PartitionFetchRequest,
    output_bytes: usize,
    response_session_id: i32,
    include_partition: bool,
) {
    let fence = request.fence();
    executor
        .reserve_output_for_test(fence, output_bytes)
        .unwrap_or_else(|error| panic!("reserve session output: {error:?}"));
    let partition_index = include_partition.then(|| {
        i32::try_from(fence.position().partition().partition().get())
            .unwrap_or_else(|error| panic!("session partition must fit i32: {error}"))
    });
    executor
        .tracked_calls_for_test()
        .install_success_terminal_for_test(
            request,
            Moment::from_tick(7),
            12,
            response_session_id,
            partition_index,
            None,
        );
}
